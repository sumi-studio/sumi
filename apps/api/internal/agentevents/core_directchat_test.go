package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// These tests drive the core-backed Direct Chat adapter against a real
// Postgres core store and real file-backed command/event logs — the same
// primitives production wiring uses. The "host" is simulated with the same
// AcquireWriter/LoadTurn/CommitTurn calls the TypeScript secretary host
// makes; no Rust runtime or gateway generation is involved anywhere.

type coreDirectChatFixture struct {
	t       *testing.T
	ctx     context.Context
	core    *agentstate.Store
	adapter *CoreDirectChat
	gateway *DurableGateway
	pa      string
	user    string
}

func newCoreDirectChatFixture(t *testing.T) *coreDirectChatFixture {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	core := agentstate.NewStore(pool)
	commands, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatalf("open command store: %v", err)
	}
	gateway, err := OpenDurableGateway(privateRuntimeDir(t), commands)
	if err != nil {
		t.Fatalf("open gateway: %v", err)
	}
	pa := pid7(t)
	humanID := pid7(t)
	// The persona's bound human is who may decide its approvals — the
	// production adapter binds from the agents row; the test binds directly.
	if _, err := pool.Exec(context.Background(),
		"INSERT INTO humans (human_id) VALUES ($1)", humanID); err != nil {
		t.Fatalf("insert human: %v", err)
	}
	if _, _, err := core.EnsurePersona(context.Background(), pa, &humanID, "Test Secretary"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	adapter := &CoreDirectChat{Core: core, Gateway: gateway}
	return &coreDirectChatFixture{
		t:       t,
		ctx:     context.Background(),
		core:    core,
		adapter: adapter,
		gateway: gateway,
		pa:      pa,
		user:    humanID,
	}
}

func pid7(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuidv7: %v", err)
	}
	return id.String()
}

func (f *coreDirectChatFixture) provenance() DirectChatProvenance {
	return DirectChatProvenance{
		Version:            1,
		TenantID:           "tenant-1",
		PersonalityAgentID: f.pa,
		Actor:              ProvenanceActor{Kind: "human", PrincipalID: f.user},
		Source:             ProvenanceSource{Surface: "direct_chat"},
	}
}

func (f *coreDirectChatFixture) sendMessage(t *testing.T, key, text string) CommandEnvelope {
	t.Helper()
	env, err := f.adapter.Append(f.ctx, f.provenance(), key,
		json.RawMessage(fmt.Sprintf(`{"type":"user_message","text":%q,"attachments":[]}`, text)))
	if err != nil {
		t.Fatalf("append user_message: %v", err)
	}
	return env
}

// runHostTurn simulates one core-host turn: claim the queued input under a
// fresh writer generation and commit the journal events plus outcome —
// exactly the calls the TypeScript host makes through the state service.
func (f *coreDirectChatFixture) runHostTurn(t *testing.T, events []agentstate.EventInput, req agentstate.CommitRequest) (agentstate.LoadResult, agentstate.Turn) {
	t.Helper()
	lease, err := f.core.AcquireWriter(f.ctx, f.pa, "test-host", time.Minute)
	if err != nil {
		t.Fatalf("acquire writer: %v", err)
	}
	loaded, err := f.core.LoadTurn(f.ctx, f.pa, lease.Generation, "", 100)
	if err != nil {
		t.Fatalf("load turn: %v", err)
	}
	if loaded.Turn == nil {
		t.Fatalf("no queued input claimed")
	}
	req.Events = events
	turn, err := f.core.CommitTurn(f.ctx, f.pa, loaded.Turn.TurnID, lease.Generation, req)
	if err != nil {
		t.Fatalf("commit turn: %v", err)
	}
	if err := f.core.ReleaseWriter(f.ctx, f.pa, "test-host", lease.Generation); err != nil {
		t.Fatalf("release writer: %v", err)
	}
	return loaded, *turn
}

func (f *coreDirectChatFixture) inputReceived(in agentstate.Input, attempt int) agentstate.EventInput {
	return agentstate.EventInput{
		Kind: "input_received",
		Payload: map[string]any{
			"input_id":       in.InputID,
			"kind":           in.Kind,
			"text":           in.Payload["text"],
			"actor_kind":     in.ActorKind,
			"actor_id":       in.ActorID,
			"source_surface": in.SourceSurface,
			"attention":      in.Attention,
			"occurred_at":    in.OccurredAt,
			"attempt":        attempt,
		},
	}
}

func (f *coreDirectChatFixture) sweep(t *testing.T) {
	t.Helper()
	if err := f.adapter.syncPersona(f.ctx, f.pa); err != nil {
		t.Fatalf("sync persona: %v", err)
	}
}

func durableEvents(t *testing.T, g *DurableGateway, pa string) []Envelope {
	t.Helper()
	events, err := g.EventCatchUp(context.Background(), pa, 0)
	if err != nil {
		t.Fatalf("event catch-up: %v", err)
	}
	return events
}

func eventTypes(events []Envelope) []string {
	var out []string
	for _, e := range events {
		var head struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(e.Event, &head); err != nil {
			continue
		}
		out = append(out, head.Type)
	}
	return out
}

func messageTexts(events []Envelope, role string) []string {
	var out []string
	for _, e := range events {
		var ev struct {
			Type    string `json:"type"`
			Message struct {
				Role    string `json:"role"`
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal(e.Event, &ev); err != nil || ev.Type != "message_end" {
			continue
		}
		if ev.Message.Role != role {
			continue
		}
		for _, c := range ev.Message.Content {
			if c.Type == "text" {
				out = append(out, c.Text)
			}
		}
	}
	return out
}

func TestCoreDirectChatCommandBecomesDurableInput(t *testing.T) {
	f := newCoreDirectChatFixture(t)

	env := f.sendMessage(t, "key-1", "hello secretary")
	if env.Seq != 1 || env.CommandID == "" {
		t.Fatalf("unexpected envelope: %+v", env)
	}

	in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID)
	if err != nil {
		t.Fatalf("core input missing: %v", err)
	}
	if in.Status != "queued" || in.Kind != "message" ||
		in.SourceSurface != "direct_chat" || in.ActorKind != "human" ||
		in.ActorID != f.user || in.Payload["text"] != "hello secretary" {
		t.Fatalf("core input fields: %+v", in)
	}

	// Idempotent replay returns the same durable command and does not mint
	// a second input.
	again, existing, err := f.adapter.AppendWithIdempotencyStatus(
		f.ctx, f.provenance(), "key-1",
		json.RawMessage(`{"type":"user_message","text":"hello secretary","attachments":[]}`))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !existing || again.CommandID != env.CommandID {
		t.Fatalf("replay should return the same durable command, got %+v existing=%v", again, existing)
	}
	if _, err := f.core.PersonaState(f.ctx, f.pa); err != nil {
		t.Fatalf("persona state: %v", err)
	}
}

func TestCoreDirectChatProjectionDeliversReply(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	env := f.sendMessage(t, "key-1", "hello")

	// Admission alone emits nothing: the user message appears when the core
	// journals the receipt, matching the legacy runtime's semantics.
	f.sweep(t)
	events := durableEvents(t, f.gateway, f.pa)
	if got := eventTypes(events); len(got) != 1 || got[0] != "command_disposition" {
		t.Fatalf("before host turn: %v", got)
	}

	in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID)
	if err != nil {
		t.Fatalf("get input: %v", err)
	}
	loaded, _ := f.runHostTurn(t, []agentstate.EventInput{
		f.inputReceived(in, 1),
		{Kind: "assistant_message", Payload: map[string]any{"text": "hi there", "round": 0}},
	}, agentstate.CommitRequest{
		Outcome: "complete",
		Usage:   map[string]any{"rounds": []any{map[string]any{"input": 10, "output": 5, "total_tokens": 15}}},
	})
	_ = loaded

	f.sweep(t)
	events = durableEvents(t, f.gateway, f.pa)
	got := eventTypes(events)
	want := []string{"command_disposition", "message_end", "message_start", "message_end"}
	if len(got) != len(want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events %v, want %v", got, want)
		}
	}
	if texts := messageTexts(events, "user"); len(texts) != 1 || texts[0] != "hello" {
		t.Fatalf("user messages: %v", texts)
	}
	if texts := messageTexts(events, "assistant"); len(texts) != 1 || texts[0] != "hi there" {
		t.Fatalf("assistant messages: %v", texts)
	}

	// Re-sweeping and a fresh adapter (API restart) must emit nothing new —
	// projected events are content-deduped against the durable log.
	f.sweep(t)
	restarted := &CoreDirectChat{Core: f.core, Gateway: f.gateway}
	if err := restarted.syncPersona(f.ctx, f.pa); err != nil {
		t.Fatalf("resync after restart: %v", err)
	}
	if after := durableEvents(t, f.gateway, f.pa); len(after) != len(events) {
		t.Fatalf("resync appended %d duplicate events", len(after)-len(events))
	}
}

func TestCoreDirectChatQueuesWhileHostDown(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	f.sendMessage(t, "key-1", "queued while down")
	f.sweep(t) // no host: input stays queued, no error

	// Restart path: a fresh adapter over the same stores must still find the
	// persona and reconcile the queued command.
	restarted := &CoreDirectChat{Core: f.core, Gateway: f.gateway}
	if err := restarted.syncPersona(f.ctx, f.pa); err != nil {
		t.Fatalf("resync: %v", err)
	}
	ids, err := restarted.Core.PersonaIDsByInputSurface(f.ctx, "direct_chat")
	if err != nil || len(ids) != 1 || ids[0] != f.pa {
		t.Fatalf("surface discovery: %v %v", ids, err)
	}

	in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+durableEventsCommandID(t, f, 1))
	if err != nil {
		t.Fatalf("input: %v", err)
	}
	f.runHostTurn(t, []agentstate.EventInput{
		f.inputReceived(in, 1),
		{Kind: "assistant_message", Payload: map[string]any{"text": "caught up", "round": 0}},
	}, agentstate.CommitRequest{Outcome: "complete"})

	if err := restarted.syncPersona(f.ctx, f.pa); err != nil {
		t.Fatalf("post-restart sync: %v", err)
	}
	events := durableEvents(t, f.gateway, f.pa)
	if texts := messageTexts(events, "assistant"); len(texts) != 1 || texts[0] != "caught up" {
		t.Fatalf("assistant messages after restart: %v", texts)
	}
}

func durableEventsCommandID(t *testing.T, f *coreDirectChatFixture, seq uint64) string {
	t.Helper()
	commands, err := f.gateway.commands.CatchUp(f.ctx, f.pa, 1)
	if err != nil || len(commands) == 0 {
		t.Fatalf("commands: %v %v", commands, err)
	}
	return commands[0].CommandID
}

func TestCoreDirectChatFailedTurnSurfacesError(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	env := f.sendMessage(t, "key-1", "hello")
	in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID)
	if err != nil {
		t.Fatalf("input: %v", err)
	}
	f.runHostTurn(t, []agentstate.EventInput{f.inputReceived(in, 1)},
		agentstate.CommitRequest{Outcome: "fail", Error: "model unavailable", Retryable: false})

	f.sweep(t)
	events := durableEvents(t, f.gateway, f.pa)
	var found bool
	for _, e := range events {
		var ev struct {
			Type    string `json:"type"`
			Message struct {
				Role         string  `json:"role"`
				StopReason   string  `json:"stop_reason"`
				ErrorMessage *string `json:"error_message"`
			} `json:"message"`
		}
		if json.Unmarshal(e.Event, &ev) == nil && ev.Type == "message_end" &&
			ev.Message.Role == "assistant" && ev.Message.StopReason == "error" {
			found = true
			if ev.Message.ErrorMessage == nil || *ev.Message.ErrorMessage != "model unavailable" {
				t.Fatalf("error message payload: %v", ev.Message.ErrorMessage)
			}
		}
	}
	if !found {
		t.Fatalf("no terminal error message projected: %v", eventTypes(events))
	}
}

func TestCoreDirectChatDoesNotProjectOtherSurfaces(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, _, err := f.core.EnsurePersona(f.ctx, f.pa, nil, ""); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	now := time.Now().UTC()
	in, _, err := f.core.SubmitInput(f.ctx, &agentstate.Input{
		PersonaID: f.pa, InputID: "msg-1", Kind: "message",
		Payload:       map[string]any{"text": "workspace traffic"},
		ActorKind:     "human",
		ActorID:       "other-human",
		SourceSurface: "messaging",
		OccurredAt:    &now,
	})
	if err != nil {
		t.Fatalf("submit messaging input: %v", err)
	}
	f.runHostTurn(t, []agentstate.EventInput{
		f.inputReceived(in, 1),
		{Kind: "assistant_message", Payload: map[string]any{"text": "channel reply", "round": 0}},
	}, agentstate.CommitRequest{Outcome: "complete"})

	f.sweep(t)
	if events := durableEvents(t, f.gateway, f.pa); len(events) != 0 {
		t.Fatalf("messaging turn leaked into direct-chat log: %v", eventTypes(events))
	}
}

func TestCoreDirectChatApprovalDecision(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	env := f.sendMessage(t, "key-1", "note this")
	in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID)
	if err != nil {
		t.Fatalf("input: %v", err)
	}

	// Host claims the input, plans an elevated journal.note call, and parks
	// on the resulting approval.
	lease, err := f.core.AcquireWriter(f.ctx, f.pa, "test-host", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	loaded, err := f.core.LoadTurn(f.ctx, f.pa, lease.Generation, "", 100)
	if err != nil || loaded.Turn == nil {
		t.Fatalf("load: %v", err)
	}
	if _, _, err := f.core.SavePlan(f.ctx, f.pa, loaded.Turn.TurnID, lease.Generation, 0,
		agentstate.Decision{Calls: []agentstate.PlanCall{{
			Tool: "journal.note", Route: "elevated",
			Request: map[string]any{"text": "x"},
		}}}); err != nil {
		t.Fatalf("save plan: %v", err)
	}
	_, approval, _, err := f.core.ClaimOperation(f.ctx, f.pa, loaded.Turn.TurnID,
		lease.Generation, "op-1", "journal.note", 0, map[string]any{"text": "x"})
	if err != nil {
		t.Fatalf("claim gated op: %v", err)
	}
	if approval == nil {
		t.Fatal("expected a pending approval")
	}
	if _, err := f.core.CommitTurn(f.ctx, f.pa, loaded.Turn.TurnID, lease.Generation,
		agentstate.CommitRequest{
			Outcome: "await",
			Events: []agentstate.EventInput{
				f.inputReceived(in, 1),
				{Kind: "tool_call", Payload: map[string]any{"tool": "journal.note", "call_id": "call-1", "request": map[string]any{"text": "x"}, "route": "elevated"}},
				{Kind: "approval_requested", Payload: map[string]any{"tool": "journal.note", "call_id": "call-1", "route": "elevated", "approval_id": approval.ApprovalID, "request": map[string]any{"text": "x"}}},
			},
		}); err != nil {
		t.Fatalf("await commit: %v", err)
	}

	f.sweep(t)
	events := durableEvents(t, f.gateway, f.pa)
	types := eventTypes(events)
	var hasApproval bool
	for _, e := range events {
		var ev struct {
			Type    string `json:"type"`
			Request struct {
				ID string `json:"id"`
			} `json:"request"`
		}
		if json.Unmarshal(e.Event, &ev) == nil && ev.Type == "approval_requested" {
			hasApproval = true
			if ev.Request.ID != approval.ApprovalID {
				t.Fatalf("approval id: %q want %q", ev.Request.ID, approval.ApprovalID)
			}
		}
	}
	if !hasApproval {
		t.Fatalf("approval_requested not projected: %v", types)
	}
	if !f.gateway.IsApprovalPending(f.pa, approval.ApprovalID) {
		t.Fatal("projected approval did not register pending state")
	}

	// The Human decides through an ordinary approval_decision command.
	decision := fmt.Sprintf(`{"type":"approval_decision","request_id":%q,"decision":{"type":"deny_once"}}`, approval.ApprovalID)
	if _, err := f.adapter.Append(f.ctx, f.provenance(), "key-2", json.RawMessage(decision)); err != nil {
		t.Fatalf("approval_decision command: %v", err)
	}
	f.sweep(t)
	events = durableEvents(t, f.gateway, f.pa)
	var resolved bool
	for _, e := range events {
		var ev struct {
			Type       string `json:"type"`
			RequestID  string `json:"request_id"`
			Resolution struct {
				Decision struct {
					Type string `json:"type"`
				} `json:"decision"`
			} `json:"resolution"`
		}
		if json.Unmarshal(e.Event, &ev) == nil && ev.Type == "approval_resolved" &&
			ev.RequestID == approval.ApprovalID && ev.Resolution.Decision.Type == "deny_once" {
			resolved = true
		}
	}
	if !resolved {
		t.Fatalf("approval_resolved not projected: %v", eventTypes(events))
	}
	if f.gateway.IsApprovalPending(f.pa, approval.ApprovalID) {
		t.Fatal("approval still pending after decision")
	}
}

func TestCoreDirectChatReadiness(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	r, err := f.adapter.DirectChatReadiness(f.ctx, f.pa)
	if err != nil || !r.ready {
		t.Fatalf("readiness: %+v %v", r, err)
	}
}

func TestAppendProjectedEventsBlockedByLiveRuntime(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	// A hydrated legacy runtime generation owns the event log; projected
	// writes must refuse rather than interleave a second writer.
	receipt := "hydrated-1"
	if err := f.gateway.PublishRuntimeState(f.pa, 7, &receipt); err != nil {
		t.Fatalf("hydrate: %v", err)
	}
	err := f.gateway.AppendProjectedEvents(f.ctx, f.pa, []json.RawMessage{
		json.RawMessage(`{"type":"agent_start"}`),
	})
	if !errors.Is(err, errProjectedWriteBlocked) {
		t.Fatalf("append under live runtime: %v", err)
	}
}
