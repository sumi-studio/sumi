package messaging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	applicationapps "github.com/sumi-studio/sumi/apps/api/internal/apps"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

// createOwnedTestDB mirrors testdb.Create but names the database under the
// caller's owned prefix so its databases can never be confused with — or
// reused by — another worker's fixtures.
func createOwnedTestDB(t *testing.T, prefix string) *pgxpool.Pool {
	t.Helper()
	return createOwnedTestDBConns(t, prefix, 10)
}

// createOwnedTestDBConns is createOwnedTestDB with a sized pool — a
// one-connection pool proves the delegated-effect path never acquires a
// second connection while the operation claim holds its own.
func createOwnedTestDBConns(t *testing.T, prefix string, maxConns int32) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("SUMI_TEST_DB_URL"))
	if databaseURL == "" {
		t.Skip("SUMI_TEST_DB_URL not set; skipping Postgres integration test")
	}
	maintenance, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatalf("connect maintenance pool: %v", err)
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("generate db suffix: %v", err)
	}
	testDBName := prefix + hex.EncodeToString(suffix)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := maintenance.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, testDBName)); err != nil {
		maintenance.Close()
		t.Fatalf("create test database: %v", err)
	}
	testURL := sharedIntakeDBURL(databaseURL, testDBName)
	config, err := pgxpool.ParseConfig(testURL)
	if err != nil {
		dropSharedIntakeDB(maintenance, testDBName)
		t.Fatalf("parse test database config: %v", err)
	}
	config.MaxConns = maxConns
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		dropSharedIntakeDB(maintenance, testDBName)
		t.Fatalf("connect test pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		dropSharedIntakeDB(maintenance, testDBName)
		t.Fatalf("ping test database: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropSharedIntakeDB(maintenance, testDBName)
	})
	return pool
}

func dropSharedIntakeDB(maintenance *pgxpool.Pool, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := maintenance.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s" WITH (FORCE)`, name)); err != nil {
		fmt.Printf("drop test database %s: %v\n", name, err)
	}
	maintenance.Close()
}

func sharedIntakeDBURL(databaseURL, name string) string {
	if i := strings.Index(databaseURL, "?"); i >= 0 {
		return swapSharedIntakePath(databaseURL[:i]) + name + databaseURL[i:]
	}
	return swapSharedIntakePath(databaseURL) + name
}

func swapSharedIntakePath(prefix string) string {
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		return prefix[:i+1]
	}
	return prefix + "/"
}

func createSharedIntakeDB(t *testing.T) *pgxpool.Pool {
	return createOwnedTestDB(t, "sumi_shared_intake_")
}

// newSharedIntakeWorld mirrors newWorld on the owned database: two humans
// (Yohaku, Haru) and one secretary (Kuro), synthetic and disposable.
func newSharedIntakeWorld(t *testing.T, ctx context.Context) world {
	t.Helper()
	return newWorldOnPool(t, ctx, createSharedIntakeDB(t))
}

func newWorldOnPool(t *testing.T, ctx context.Context, pool *pgxpool.Pool) world {
	t.Helper()
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	registry := koseki.New(pool)
	humanA, err := registry.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human A: %v", err)
	}
	humanB, err := registry.MintHuman(ctx)
	if err != nil {
		t.Fatalf("mint human B: %v", err)
	}
	agent, err := registry.MintSecretary(ctx, humanA)
	if err != nil {
		t.Fatalf("mint agent: %v", err)
	}
	for id, name := range map[string]string{humanA: "Yohaku", humanB: "Haru"} {
		if _, err := pool.Exec(ctx, "UPDATE humans SET display_name = $1 WHERE human_id = $2", name, id); err != nil {
			t.Fatalf("name human: %v", err)
		}
	}
	if _, err := pool.Exec(ctx, "UPDATE agents SET display_name = 'Kuro' WHERE personality_agent_id = $1", agent); err != nil {
		t.Fatalf("name agent: %v", err)
	}
	workspaces := workspacecontrol.New(pool)
	apps := applicationapps.New(pool, workspaces)
	core := New(pool, workspaces, apps)
	store := &testMessagingStore{Store: core, core: core, workspaces: workspaces, apps: apps}
	registerTestStore(store, Human(humanA), Human(humanB), PersonalityAgent(agent))
	return world{
		store: store, workspaces: workspaces, apps: apps,
		humanA: Human(humanA), humanB: Human(humanB), agent: PersonalityAgent(agent),
	}
}

func newSharedIntakeDelivery(t *testing.T, w world) (*CoreAttentionDelivery, *agentstate.Store) {
	t.Helper()
	coreStore := agentstate.NewStore(w.store.core.pool)
	delivery := &CoreAttentionDelivery{Core: coreStore, Messaging: w.store.core}
	if err := coreStore.RegisterEffect(MessagingCoreTool, delivery.SendEffect()); err != nil {
		t.Fatalf("register messaging.send effect: %v", err)
	}
	return delivery, coreStore
}

// coreInputFor reads the admitted input for one event recipient.
func coreInputFor(t *testing.T, ctx context.Context, w world, paID string) agentstate.Input {
	t.Helper()
	rows, err := w.store.core.pool.Query(ctx,
		`SELECT input_id, kind, actor_kind, actor_id, source_surface, thread_id,
		        occurred_at, attention, status, payload
		   FROM core_inputs WHERE persona_id = $1`, paID)
	if err != nil {
		t.Fatalf("read core inputs: %v", err)
	}
	defer rows.Close()
	var inputs []agentstate.Input
	for rows.Next() {
		var in agentstate.Input
		var occurred *time.Time
		if err := rows.Scan(&in.InputID, &in.Kind, &in.ActorKind, &in.ActorID,
			&in.SourceSurface, &in.ThreadID, &occurred, &in.Attention, &in.Status,
			&in.Payload); err != nil {
			t.Fatalf("scan core input: %v", err)
		}
		in.OccurredAt = occurred
		in.PersonaID = paID
		inputs = append(inputs, in)
	}
	if len(inputs) != 1 {
		t.Fatalf("persona %s has %d core inputs, want 1", paID, len(inputs))
	}
	return inputs[0]
}

func TestSharedIntakeDMDeliversProvenanceToCore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newSharedIntakeWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	msg := w.send(t, ctx, dm.PlaceID, w.humanA, "駅前で待ち合わせ")
	delivery, coreStore := newSharedIntakeDelivery(t, w)

	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if stats.Admitted != 1 {
		t.Fatalf("admitted %d events, want 1", stats.Admitted)
	}

	in := coreInputFor(t, ctx, w, w.agent.ID)
	if in.InputID == "" || in.Kind != "message" || in.Status != "queued" {
		t.Fatalf("input = %+v", in)
	}
	if in.ActorKind != "human" || in.ActorID != w.humanA.ID {
		t.Fatalf("actor = %s/%s, want human/%s", in.ActorKind, in.ActorID, w.humanA.ID)
	}
	if in.SourceSurface != "messaging" || in.ThreadID != dm.PlaceID {
		t.Fatalf("surface/thread = %q/%q", in.SourceSurface, in.ThreadID)
	}
	if in.Attention != "reply" {
		t.Fatalf("DM attention = %q, want reply", in.Attention)
	}
	if in.OccurredAt == nil || !in.OccurredAt.Equal(msg.CreatedAt) {
		t.Fatalf("occurred_at = %v, want message created_at %v", in.OccurredAt, msg.CreatedAt)
	}
	if got := in.Payload["text"]; got != "駅前で待ち合わせ" {
		t.Fatalf("payload text = %v", got)
	}
	actor, _ := in.Payload["actor"].(map[string]any)
	if actor["display_name"] != "Yohaku" || actor["id"] != w.humanA.ID {
		t.Fatalf("actor provenance = %+v", actor)
	}
	place, _ := in.Payload["place"].(map[string]any)
	if place["id"] != dm.PlaceID || place["kind"] != "dm" {
		t.Fatalf("place provenance = %+v", place)
	}
	if in.Payload["reason"] != NotifyReasonDM || in.Payload["message_id"] != msg.MessageID ||
		in.Payload["message_seq"] != float64(msg.Seq) {
		t.Fatalf("message provenance = %+v", in.Payload)
	}
	// Persona carries the agent's owning human and name.
	persona, err := coreStore.PersonaState(ctx, w.agent.ID)
	if err != nil {
		t.Fatalf("persona state: %v", err)
	}
	if persona.Persona.DisplayName != "Kuro" || persona.Persona.HumanID == nil ||
		*persona.Persona.HumanID != w.humanA.ID {
		t.Fatalf("persona = %+v", persona.Persona)
	}
	// The delivery row carries the receipt: command id is the event id.
	var receiptID string
	var receiptSeq int64
	var eventID string
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT event_id::text, admitted_command_id::text, admitted_command_seq
		   FROM agent_attention_deliveries WHERE message_id = $1`, msg.MessageID).
		Scan(&eventID, &receiptID, &receiptSeq); err != nil {
		t.Fatalf("delivery receipt: %v", err)
	}
	if receiptID != eventID || receiptSeq <= 0 {
		t.Fatalf("receipt = %s/%d", receiptID, receiptSeq)
	}

	// A lost receipt acknowledgement reconciles through Lookup without a
	// second input: clear the admission columns, drain again.
	if _, err := w.store.core.pool.Exec(ctx,
		`UPDATE agent_attention_deliveries
		    SET admitted_at = NULL, admitted_command_id = NULL, admitted_command_seq = NULL
		  WHERE event_id = $1`, eventID); err != nil {
		t.Fatalf("un-admit: %v", err)
	}
	stats, err = w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil {
		t.Fatalf("re-deliver: %v", err)
	}
	if stats.Admitted != 1 {
		t.Fatalf("re-admitted %d, want 1", stats.Admitted)
	}
	var inputCount int
	if err := w.store.core.pool.QueryRow(ctx,
		"SELECT count(*) FROM core_inputs WHERE persona_id = $1", w.agent.ID).Scan(&inputCount); err != nil {
		t.Fatalf("count inputs: %v", err)
	}
	if inputCount != 1 {
		t.Fatalf("core inputs after reconcile = %d, want 1", inputCount)
	}
	// Fully drained rows are skipped, not re-admitted.
	stats, err = w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil {
		t.Fatalf("third drain: %v", err)
	}
	if stats.Admitted != 0 || stats.Retried != 0 {
		t.Fatalf("final drain stats = %+v", stats)
	}
}

func TestSharedIntakeAttentionHintFollowsNotificationReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newSharedIntakeWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, _ := newSharedIntakeDelivery(t, w)

	// Ambient channel traffic arrives for awareness, not as a reply demand.
	w.send(t, ctx, ch.PlaceID, w.humanB, "みんなへの周知")
	if _, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil {
		t.Fatalf("drain ambient: %v", err)
	}
	if in := coreInputFor(t, ctx, w, w.agent.ID); in.Attention != "observe" {
		t.Fatalf("ambient attention = %q, want observe", in.Attention)
	}
	if _, err := w.store.core.pool.Exec(ctx,
		"DELETE FROM core_inputs WHERE persona_id = $1", w.agent.ID); err != nil {
		t.Fatal(err)
	}

	// A mention asks for the secretary directly.
	w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 見てくれる？")
	if _, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil {
		t.Fatalf("drain mention: %v", err)
	}
	if in := coreInputFor(t, ctx, w, w.agent.ID); in.Attention != "reply" {
		t.Fatalf("mention attention = %q, want reply", in.Attention)
	}
	if _, err := w.store.core.pool.Exec(ctx,
		"DELETE FROM core_inputs WHERE persona_id = $1", w.agent.ID); err != nil {
		t.Fatal(err)
	}

	// A reply to the secretary's own message is conversational.
	own := w.send(t, ctx, ch.PlaceID, w.agent, "先に共有しておく")
	scoped := w.store.mustScopeForPlace(t, ctx, ch.PlaceID, w.humanB)
	if _, _, err := scoped.AppendMessage(ctx, AppendInput{
		PlaceID: ch.PlaceID, Content: "了解、あとで読む", ReplyTo: own.MessageID,
		ClientNonce: "human-reply-1",
	}); err != nil {
		t.Fatalf("reply send: %v", err)
	}
	if _, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil {
		t.Fatalf("drain reply: %v", err)
	}
	in := coreInputFor(t, ctx, w, w.agent.ID)
	if in.Attention != "reply" || in.Payload["reply_to_message_id"] != own.MessageID {
		t.Fatalf("reply attention = %q payload=%+v", in.Attention, in.Payload)
	}
}

func TestSharedIntakeMultipleSecretariesShareOneConversation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newSharedIntakeWorld(t, ctx)
	// A second secretary belonging to Haru joins the same workspace.
	secondID, err := koseki.New(w.store.core.pool).MintSecretary(ctx, w.humanB.ID)
	if err != nil {
		t.Fatalf("mint second secretary: %v", err)
	}
	second := PersonalityAgent(secondID)
	if _, err := w.store.core.pool.Exec(ctx,
		"UPDATE agents SET display_name='Shiro' WHERE personality_agent_id=$1", secondID); err != nil {
		t.Fatal(err)
	}
	ws, ch := w.workspaceWithChannel(t, ctx)
	if err := w.store.AddWorkspaceMember(ctx, ws.WorkspaceID, second, RoleMember); err != nil {
		t.Fatalf("add second secretary: %v", err)
	}
	delivery, _ := newSharedIntakeDelivery(t, w)

	msg := w.send(t, ctx, ch.PlaceID, w.humanA, "@Kuro（Yohaku） @Shiro（Haru） ふたりとも読んで")
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if stats.Admitted != 2 {
		t.Fatalf("admitted %d events, want 2 (one per secretary)", stats.Admitted)
	}
	for _, pa := range []ParticipantRef{w.agent, second} {
		in := coreInputFor(t, ctx, w, pa.ID)
		if in.Payload["message_id"] != msg.MessageID || in.ThreadID != ch.PlaceID {
			t.Fatalf("persona %s input = %+v", pa.ID, in)
		}
		if in.ActorID != w.humanA.ID || in.Attention != "reply" {
			t.Fatalf("persona %s actor/attention = %s/%s", pa.ID, in.ActorID, in.Attention)
		}
	}
}

func TestSharedIntakeStoppedRuntimeDeliversInOrderAfterReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	w := newSharedIntakeWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, coreStore := newSharedIntakeDelivery(t, w)

	// The secretary runtime is stopped: messages admitted now must stay
	// queued and arrive in order when a writer next acquires the lease.
	first := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 一つ目")
	second := w.send(t, ctx, ch.PlaceID, w.humanB, "@Kuro（Yohaku） 二つ目")
	if _, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil {
		t.Fatalf("drain while stopped: %v", err)
	}
	var queued int
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND status='queued'`,
		w.agent.ID).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 2 {
		t.Fatalf("queued inputs while stopped = %d, want 2", queued)
	}
	// The received wall-clock time is not the order: a database clock that
	// steps backward between the two admissions (seen on the WSL2 host, where
	// it once swapped these inputs) must not let the second message run first.
	if _, err := w.store.core.pool.Exec(ctx,
		`UPDATE core_inputs SET created_at = created_at - interval '1 hour'
		  WHERE persona_id=$1 AND payload->>'message_id'=$2`,
		w.agent.ID, second.MessageID); err != nil {
		t.Fatalf("step second input's received time back: %v", err)
	}

	// Runtime A claims the first input then dies before committing — a stop,
	// not a secretary death. The expired lease lets runtime B take over.
	leaseA, err := coreStore.AcquireWriter(ctx, w.agent.ID, "runtime-a", 150*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	res, err := coreStore.LoadTurn(ctx, w.agent.ID, leaseA.Generation, "turn-a1", 20)
	if err != nil {
		t.Fatalf("load turn A: %v", err)
	}
	if res.Turn == nil || res.Input == nil || res.Input.Payload["message_id"] != first.MessageID {
		t.Fatalf("first claimed input = %+v", res.Input)
	}
	time.Sleep(250 * time.Millisecond) // lease expiry == stopped runtime

	leaseB, err := coreStore.AcquireWriter(ctx, w.agent.ID, "runtime-b", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire B after stop: %v", err)
	}
	rec, err := coreStore.Recover(ctx, w.agent.ID, leaseB.Generation)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(rec.RequeuedInputs) != 1 || len(rec.InterruptedTurns) != 1 {
		t.Fatalf("recovery = %+v", rec)
	}

	// Reconnected delivery order: the interrupted first input again, then the
	// second — no input skipped, none delivered twice to a committed turn.
	res, err = coreStore.LoadTurn(ctx, w.agent.ID, leaseB.Generation, "turn-b1", 20)
	if err != nil {
		t.Fatalf("load turn B1: %v", err)
	}
	if res.Input == nil || res.Input.Payload["message_id"] != first.MessageID {
		t.Fatalf("reconnect first input = %+v", res.Input)
	}
	if _, err := coreStore.CommitTurn(ctx, w.agent.ID, res.Turn.TurnID, leaseB.Generation,
		agentstate.CommitRequest{Outcome: "complete", Output: map[string]any{"text": "ok"}}); err != nil {
		t.Fatalf("commit B1: %v", err)
	}
	res, err = coreStore.LoadTurn(ctx, w.agent.ID, leaseB.Generation, "turn-b2", 20)
	if err != nil {
		t.Fatalf("load turn B2: %v", err)
	}
	if res.Input == nil || res.Input.Payload["message_id"] != second.MessageID {
		t.Fatalf("reconnect second input = %+v", res.Input)
	}
	if _, err := coreStore.CommitTurn(ctx, w.agent.ID, res.Turn.TurnID, leaseB.Generation,
		agentstate.CommitRequest{Outcome: "complete", Output: map[string]any{"text": "ok"}}); err != nil {
		t.Fatalf("commit B2: %v", err)
	}
	var done int
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_inputs WHERE persona_id=$1 AND status='done'`,
		w.agent.ID).Scan(&done); err != nil {
		t.Fatal(err)
	}
	if done != 2 {
		t.Fatalf("done inputs = %d, want 2", done)
	}
}

func TestCoreSendEffectPostsIntoRealPlace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newSharedIntakeWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, coreStore := newSharedIntakeDelivery(t, w)
	release, err := delivery.Prepare(ctx, w.agent.ID)
	if err != nil {
		t.Fatalf("prepare persona: %v", err)
	}
	release()

	// A turn for one admitted input, with a recorded plan calling
	// messaging.send — the same sequence the TypeScript secretary executes.
	occurred := time.Now()
	if _, _, err := coreStore.SubmitInput(ctx, &agentstate.Input{
		PersonaID: w.agent.ID, InputID: "messaging:test-send-1", Kind: "message",
		Payload:   map[string]any{"text": "post for me"},
		ActorKind: "human", ActorID: w.humanA.ID, SourceSurface: "messaging",
		ThreadID: ch.PlaceID, OccurredAt: &occurred, Attention: "reply",
	}); err != nil {
		t.Fatalf("submit input: %v", err)
	}
	lease, err := coreStore.AcquireWriter(ctx, w.agent.ID, "runtime", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	res, err := coreStore.LoadTurn(ctx, w.agent.ID, lease.Generation, "turn-1", 20)
	if err != nil || res.Turn == nil {
		t.Fatalf("load turn: %v %+v", err, res)
	}
	sendReq := map[string]any{"place_id": ch.PlaceID, "content": "共有できました"}
	if _, _, err := coreStore.SavePlan(ctx, w.agent.ID, res.Turn.TurnID, lease.Generation, 0,
		agentstate.Decision{
			Text:  "posting",
			Calls: []agentstate.PlanCall{{CallID: "c1", Tool: MessagingCoreTool, Route: "normal", Request: sendReq}},
		}); err != nil {
		t.Fatalf("save plan: %v", err)
	}
	op, _, fresh, err := coreStore.ClaimOperation(ctx, w.agent.ID, res.Turn.TurnID,
		lease.Generation, "turn-1:op:0", MessagingCoreTool, 0, sendReq)
	if err != nil {
		t.Fatalf("claim send: %v", err)
	}
	if !fresh || op.Status != "done" {
		t.Fatalf("op = %+v fresh=%t", op, fresh)
	}
	messageID, _ := op.Response["message_id"].(string)
	if messageID == "" {
		t.Fatalf("send response = %+v", op.Response)
	}
	// The message is committed in the real place, authored by the secretary.
	history, err := w.store.History(ctx, ch.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	var posted Message
	for _, m := range history {
		if m.MessageID == messageID {
			posted = m
		}
	}
	if posted.Author != w.agent || posted.Content != "共有できました" || posted.Seq != 1 {
		t.Fatalf("posted message = %+v", posted)
	}
	// The send issued the same durable notification intents a human's send
	// would — the other members get their own attention events.
	var intents int
	if err := w.store.core.pool.QueryRow(ctx,
		"SELECT count(*) FROM message_notification_intents WHERE message_id=$1", messageID).Scan(&intents); err != nil {
		t.Fatal(err)
	}
	if intents < 2 {
		t.Fatalf("notification intents = %d, want the two other members", intents)
	}

	// A replayed claim returns the stored receipt — no second message.
	again, _, freshAgain, err := coreStore.ClaimOperation(ctx, w.agent.ID, res.Turn.TurnID,
		lease.Generation, "turn-1:op:0", MessagingCoreTool, 0, sendReq)
	if err != nil {
		t.Fatalf("replay claim: %v", err)
	}
	if freshAgain || again.Response["message_id"] != messageID {
		t.Fatalf("replay = %+v fresh=%t", again, freshAgain)
	}
	if place, err := w.store.PlaceFor(ctx, ch.PlaceID, w.humanA); err != nil || place.LastSeq != 1 {
		t.Fatalf("place after replay = %+v %v", place, err)
	}

	// Deterministic failure (a place the secretary cannot see) is a recorded
	// tool error, not a transient retry: it maps to ErrBadRequest.
	other := newSharedIntakeIsolatedPlace(t, ctx, w)
	badReq := map[string]any{"place_id": other, "content": "届かない"}
	if _, _, err := coreStore.SavePlan(ctx, w.agent.ID, res.Turn.TurnID, lease.Generation, 1,
		agentstate.Decision{
			Text:  "trying elsewhere",
			Calls: []agentstate.PlanCall{{CallID: "c2", Tool: MessagingCoreTool, Route: "normal", Request: badReq}},
		}); err != nil {
		t.Fatalf("save plan 2: %v", err)
	}
	if _, _, _, err := coreStore.ClaimOperation(ctx, w.agent.ID, res.Turn.TurnID,
		lease.Generation, "turn-1:op:1", MessagingCoreTool, 1, badReq); !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("inaccessible place claim: got %v, want ErrBadRequest", err)
	}
}

// An elevated-route messaging.send is a recorded outward act the human
// must approve: the claim parks behind a durable pending approval, the
// bound human's decision releases exactly one send, and a denial posts
// nothing — under the same registered effect the normal route uses.
func TestCoreSendEffectElevatedApprovalGatesTheSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newSharedIntakeWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	delivery, coreStore := newSharedIntakeDelivery(t, w)
	release, err := delivery.Prepare(ctx, w.agent.ID)
	if err != nil {
		t.Fatalf("prepare persona: %v", err)
	}
	release()

	occurred := time.Now()
	if _, _, err := coreStore.SubmitInput(ctx, &agentstate.Input{
		PersonaID: w.agent.ID, InputID: "messaging:test-elev-1", Kind: "message",
		Payload:   map[string]any{"text": "post for me"},
		ActorKind: "human", ActorID: w.humanA.ID, SourceSurface: "messaging",
		ThreadID: ch.PlaceID, OccurredAt: &occurred, Attention: "reply",
	}); err != nil {
		t.Fatalf("submit input: %v", err)
	}
	lease, err := coreStore.AcquireWriter(ctx, w.agent.ID, "runtime", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	res, err := coreStore.LoadTurn(ctx, w.agent.ID, lease.Generation, "turn-1", 20)
	if err != nil || res.Turn == nil {
		t.Fatalf("load turn: %v %+v", err, res)
	}
	sendReq := map[string]any{"place_id": ch.PlaceID, "content": "共有できました"}
	if _, _, err := coreStore.SavePlan(ctx, w.agent.ID, res.Turn.TurnID, lease.Generation, 0,
		agentstate.Decision{
			Text: "posting with consent",
			Calls: []agentstate.PlanCall{
				{CallID: "c1", Tool: MessagingCoreTool, Route: "elevated", Request: sendReq},
			},
		}); err != nil {
		t.Fatalf("save plan: %v", err)
	}

	// The claim parks: operation awaiting_approval, durable pending grant,
	// and no message exists while the human has not decided.
	op, appr, fresh, err := coreStore.ClaimOperation(ctx, w.agent.ID, res.Turn.TurnID,
		lease.Generation, "turn-1:op:0", MessagingCoreTool, 0, sendReq)
	if err != nil {
		t.Fatalf("claim send: %v", err)
	}
	if !fresh || op.Status != "awaiting_approval" || appr == nil ||
		appr.Status != "pending" || appr.RequiredBy != "route" {
		t.Fatalf("parked claim = op %+v appr %+v fresh=%t", op, appr, fresh)
	}
	var posted int
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT count(*) FROM messages WHERE place_id = $1`, ch.PlaceID).Scan(&posted); err != nil {
		t.Fatal(err)
	}
	if posted != 0 {
		t.Fatalf("message posted before approval: %d", posted)
	}

	// Only the persona's bound human decides — the other workspace member
	// cannot consent for this secretary.
	if _, err := coreStore.ResolveApproval(ctx, w.agent.ID, appr.ApprovalID,
		agentstate.ApprovalDecision{
			Decision: "approve_once", DecisionID: "d-wrong",
			DecidedByKind: "human", DecidedByID: w.humanB.ID,
		}); !errors.Is(err, agentstate.ErrApprovalForbidden) {
		t.Fatalf("foreign human decision: got %v, want ErrApprovalForbidden", err)
	}
	if _, err := coreStore.ResolveApproval(ctx, w.agent.ID, appr.ApprovalID,
		agentstate.ApprovalDecision{
			Decision: "approve_once", DecisionID: "d-1",
			DecidedByKind: "human", DecidedByID: w.humanA.ID,
		}); err != nil {
		t.Fatalf("approve: %v", err)
	}

	// The next claim consumes the one-shot grant and sends exactly once,
	// inside the same transaction as the operation record.
	op2, appr2, fresh2, err := coreStore.ClaimOperation(ctx, w.agent.ID, res.Turn.TurnID,
		lease.Generation, "turn-1:op:0", MessagingCoreTool, 0, sendReq)
	if err != nil {
		t.Fatalf("approved claim: %v", err)
	}
	if !fresh2 || op2.Status != "done" || appr2 == nil || appr2.ConsumedAt == nil {
		t.Fatalf("approved claim = op %+v appr %+v fresh=%t", op2, appr2, fresh2)
	}
	messageID, _ := op2.Response["message_id"].(string)
	if messageID == "" {
		t.Fatalf("send response = %+v", op2.Response)
	}
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT count(*) FROM messages WHERE place_id = $1`, ch.PlaceID).Scan(&posted); err != nil {
		t.Fatal(err)
	}
	if posted != 1 {
		t.Fatalf("posted after approval = %d, want exactly one", posted)
	}

	// Replay: the stored receipt returns, the consumed grant cannot run the
	// effect a second time.
	op3, _, fresh3, err := coreStore.ClaimOperation(ctx, w.agent.ID, res.Turn.TurnID,
		lease.Generation, "turn-1:op:0", MessagingCoreTool, 0, sendReq)
	if err != nil || fresh3 || op3.Response["message_id"] != messageID {
		t.Fatalf("replay = %+v fresh=%t err=%v", op3, fresh3, err)
	}
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT count(*) FROM messages WHERE place_id = $1`, ch.PlaceID).Scan(&posted); err != nil {
		t.Fatal(err)
	}
	if posted != 1 {
		t.Fatalf("posted after replay = %d, want still one", posted)
	}
	if _, err := coreStore.CommitTurn(ctx, w.agent.ID, res.Turn.TurnID, lease.Generation,
		agentstate.CommitRequest{Outcome: "complete", Output: map[string]any{"text": "posted"}}); err != nil {
		t.Fatalf("commit turn 1: %v", err)
	}

	// A denied elevated send posts nothing and replays the durable failure.
	occurred2 := time.Now()
	if _, _, err := coreStore.SubmitInput(ctx, &agentstate.Input{
		PersonaID: w.agent.ID, InputID: "messaging:test-elev-2", Kind: "message",
		Payload:   map[string]any{"text": "post this too"},
		ActorKind: "human", ActorID: w.humanA.ID, SourceSurface: "messaging",
		ThreadID: ch.PlaceID, OccurredAt: &occurred2, Attention: "reply",
	}); err != nil {
		t.Fatalf("submit input 2: %v", err)
	}
	res2, err := coreStore.LoadTurn(ctx, w.agent.ID, lease.Generation, "turn-2", 20)
	if err != nil || res2.Turn == nil {
		t.Fatalf("load turn 2: %v %+v", err, res2)
	}
	denyReq := map[string]any{"place_id": ch.PlaceID, "content": "却下"}
	if _, _, err := coreStore.SavePlan(ctx, w.agent.ID, res2.Turn.TurnID, lease.Generation, 0,
		agentstate.Decision{
			Text: "posting again",
			Calls: []agentstate.PlanCall{
				{CallID: "c1", Tool: MessagingCoreTool, Route: "elevated", Request: denyReq},
			},
		}); err != nil {
		t.Fatalf("save plan 2: %v", err)
	}
	op4, appr4, _, err := coreStore.ClaimOperation(ctx, w.agent.ID, res2.Turn.TurnID,
		lease.Generation, "turn-2:op:0", MessagingCoreTool, 0, denyReq)
	if err != nil || op4.Status != "awaiting_approval" || appr4 == nil {
		t.Fatalf("deny-path park = op %+v appr %+v err=%v", op4, appr4, err)
	}
	if _, err := coreStore.ResolveApproval(ctx, w.agent.ID, appr4.ApprovalID,
		agentstate.ApprovalDecision{
			Decision: "deny_once", DecisionID: "d-2",
			DecidedByKind: "human", DecidedByID: w.humanA.ID,
		}); err != nil {
		t.Fatalf("deny: %v", err)
	}
	op5, _, fresh5, err := coreStore.ClaimOperation(ctx, w.agent.ID, res2.Turn.TurnID,
		lease.Generation, "turn-2:op:0", MessagingCoreTool, 0, denyReq)
	if err != nil {
		t.Fatalf("post-denial claim: %v", err)
	}
	if fresh5 || op5.Status != "failed" {
		t.Fatalf("post-denial op = %+v fresh=%t", op5, fresh5)
	}
	var denied int
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT count(*) FROM messages WHERE place_id = $1 AND content = '却下'`,
		ch.PlaceID).Scan(&denied); err != nil {
		t.Fatal(err)
	}
	if denied != 0 {
		t.Fatalf("denied send posted %d messages", denied)
	}
}

// newSharedIntakeIsolatedPlace creates a channel in a different workspace the
// agent is not a member of.
func newSharedIntakeIsolatedPlace(t *testing.T, ctx context.Context, w world) string {
	t.Helper()
	ws, err := w.store.CreateWorkspace(ctx, "elsewhere", w.humanB)
	if err != nil {
		t.Fatalf("create other workspace: %v", err)
	}
	ch, err := w.store.CreateChannel(ctx, ws.WorkspaceID, "other", "", w.humanB)
	if err != nil {
		t.Fatalf("create other channel: %v", err)
	}
	return ch.PlaceID
}

// startSharedIntakeCoreServer mounts the real agentstate HTTP contract on an
// owned port so the real Node core can run against it.
func startSharedIntakeCoreServer(t *testing.T, w world, delivery *CoreAttentionDelivery) (*agentstate.Server, string) {
	return startOwnedCoreServer(t, w, delivery, 9530, 9539)
}

func startOwnedCoreServer(t *testing.T, w world, delivery *CoreAttentionDelivery, portLo, portHi int) (*agentstate.Server, string) {
	t.Helper()
	const token = "shared-intake-test-token-0123456789"
	srv := agentstate.NewServer(w.store.core.pool, token)
	if err := srv.RegisterToolEffect(MessagingCoreTool, delivery.SendEffect()); err != nil {
		t.Fatalf("register effect: %v", err)
	}
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	var ln net.Listener
	var err error
	for port := portLo; port <= portHi; port++ {
		ln, err = net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("no free port in %d-%d: %v", portLo, portHi, err)
	}
	httpSrv := &http.Server{Handler: mux}
	go func() { _ = httpSrv.Serve(ln) }()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	})
	return srv, "http://" + ln.Addr().String()
}

func coreRootDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test file")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "apps", "core")
}

// TestSharedIntakeEndToEndCoreReply runs the real Node secretary core against
// the real Go state service and real PostgreSQL: a human's DM becomes a
// durable core input, the mock-model secretary answers through
// messaging.send, and the reply lands in the same DM as an ordinary
// secretary-authored message — committed once even when the turn's commit is
// replayed through a second runtime start.
func TestSharedIntakeEndToEndCoreReply(t *testing.T) {
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not on PATH; skipping Node core e2e")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	w := newSharedIntakeWorld(t, ctx)
	_, ch := w.workspaceWithChannel(t, ctx)
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	delivery, _ := newSharedIntakeDelivery(t, w)
	// The human's open client is a live hub subscriber: the secretary's
	// reply must reach it as it would a human's send, not only via history.
	delivery.Hub = NewHub(w.store.core)
	viewer := delivery.Hub.subscribe(w.store.mustScopeForPlace(t, ctx, dm.PlaceID, w.humanA))
	defer delivery.Hub.unsubscribe(viewer)
	coreSrv, baseURL := startSharedIntakeCoreServer(t, w, delivery)

	replyText := "承知しました、待ち合わせです"
	directive := fmt.Sprintf(`!messaging.send {"place_id":%q,"content":%q}`, dm.PlaceID, replyText)
	w.send(t, ctx, dm.PlaceID, w.humanA, directive)

	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if stats.Admitted != 1 {
		t.Fatalf("admitted %d, want 1", stats.Admitted)
	}

	env := append(os.Environ(),
		"SUMI_STATE_URL="+baseURL,
		"SUMI_PERSONA_ID="+w.agent.ID,
		"SUMI_PERSONA_TOKEN="+coreSrv.PersonaToken(w.agent.ID),
		"SUMI_MODEL_PROVIDER=mock",
		"SUMI_LEASE_TTL_MS=4000",
		"SUMI_ONCE_IDLE_MS=800",
	)
	runOnce := func() {
		t.Helper()
		cmd := exec.CommandContext(ctx, nodePath,
			filepath.Join(coreRootDir(t), "src", "host", "local.ts"), "--once")
		cmd.Dir = coreRootDir(t)
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("core --once failed: %v\n%s", err, out)
		}
	}
	runOnce()

	// The secretary's reply is an ordinary message in the same DM.
	history, err := w.store.History(ctx, dm.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 2 || history[1].Author != w.agent || history[1].Content != replyText {
		t.Fatalf("dm history = %+v", history)
	}
	sawLive := false
	for len(viewer.send) > 0 {
		frame := <-viewer.send
		if strings.Contains(string(frame.payload), history[1].MessageID) {
			sawLive = true
		}
	}
	if !sawLive {
		t.Fatal("secretary reply was not fanned out to the human's live subscriber")
	}
	// The journal records the input with real provenance and the tool effect.
	evRows, err := w.store.core.pool.Query(ctx,
		`SELECT kind, payload FROM core_events WHERE persona_id=$1 ORDER BY seq`, w.agent.ID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	defer evRows.Close()
	var sawInput, sawToolResult bool
	for evRows.Next() {
		var kind string
		var payload map[string]any
		if err := evRows.Scan(&kind, &payload); err != nil {
			t.Fatalf("scan event: %v", err)
		}
		if kind == "input_received" {
			sawInput = true
			if payload["actor_display"] != "Yohaku" || payload["attention"] != "reply" ||
				payload["thread_id"] != dm.PlaceID || payload["source_surface"] != "messaging" {
				t.Fatalf("input_received payload = %+v", payload)
			}
		}
		if kind == "tool_result" {
			if resp, ok := payload["response"].(map[string]any); ok && resp["message_id"] == history[1].MessageID {
				sawToolResult = true
			}
		}
	}
	if !sawInput || !sawToolResult {
		t.Fatalf("journal missing input_received/tool_result: input=%t tool=%t", sawInput, sawToolResult)
	}

	// A second runtime start delivers nothing twice: the input is done and
	// the DM still holds exactly one secretary reply.
	runOnce()
	history, err = w.store.History(ctx, dm.PlaceID, w.humanA, HistoryOptions{})
	if err != nil {
		t.Fatalf("history after restart: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("dm history after restart = %d messages, want 2", len(history))
	}

	// Ambient channel traffic reaches the secretary as an experience, and the
	// runtime completes it without posting anything: turn output is journal,
	// never an implicit Messaging reply.
	ambient := w.send(t, ctx, ch.PlaceID, w.humanB, "今日は午後から全員外出です")
	if stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25); err != nil || stats.Admitted != 1 {
		t.Fatalf("deliver ambient: %+v %v", stats, err)
	}
	runOnce()
	var status, attention string
	if err := w.store.core.pool.QueryRow(ctx,
		`SELECT status, attention FROM core_inputs WHERE persona_id=$1 AND payload->>'message_id'=$2`,
		w.agent.ID, ambient.MessageID).Scan(&status, &attention); err != nil {
		t.Fatalf("ambient input: %v", err)
	}
	if status != "done" || attention != "observe" {
		t.Fatalf("ambient input status/attention = %s/%s, want done/observe", status, attention)
	}
	channelHistory, err := w.store.History(ctx, ch.PlaceID, w.humanB, HistoryOptions{})
	if err != nil {
		t.Fatalf("channel history: %v", err)
	}
	if len(channelHistory) != 1 || channelHistory[0].Author != w.humanB {
		t.Fatalf("channel history after ambient turn = %+v, want only Haru's message", channelHistory)
	}
}

// TestSharedIntakeAttentionTriggersCoreWake covers the production hop the
// earlier review never exercised: a Messaging message admitted by
// DeliverAgentAttention into core_inputs is what cmd/server's
// RuntimeWaker sweep turns into an authenticated POST /personas/:id/wake
// against the configured core host.
func TestSharedIntakeAttentionTriggersCoreWake(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w := newSharedIntakeWorld(t, ctx)
	w.workspaceWithChannel(t, ctx)
	dm, _, err := w.store.EnsureDM(ctx, w.humanA, w.agent)
	if err != nil {
		t.Fatalf("ensure dm: %v", err)
	}
	w.send(t, ctx, dm.PlaceID, w.humanA, "wake the secretary")
	delivery, coreStore := newSharedIntakeDelivery(t, w)
	stats, err := w.store.core.DeliverAgentAttention(ctx, delivery, 25)
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if stats.Admitted != 1 {
		t.Fatalf("admitted %d events, want 1", stats.Admitted)
	}

	const wakeToken = "wake-credential-for-wiring-test-01234"
	var mu sync.Mutex
	var calls []string
	host := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer "+wakeToken {
			res.WriteHeader(http.StatusUnauthorized)
			return
		}
		mu.Lock()
		calls = append(calls, req.Method+" "+req.URL.Path)
		mu.Unlock()
		res.WriteHeader(http.StatusOK)
	}))
	defer host.Close()
	waker, err := agentstate.NewRuntimeWaker(coreStore, host.URL, wakeToken)
	if err != nil {
		t.Fatalf("waker: %v", err)
	}
	if n := waker.Sweep(ctx); n != 1 {
		t.Fatalf("sweep sent %d wakes, want 1", n)
	}
	want := "POST /personas/" + w.agent.ID + "/wake"
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 || calls[0] != want {
		t.Fatalf("wake calls = %v, want [%s]", calls, want)
	}
}
