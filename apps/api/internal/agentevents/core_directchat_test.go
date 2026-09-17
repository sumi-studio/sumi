package agentevents

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
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
	pool    *pgxpool.Pool
	adapter *CoreDirectChat
	gateway *DurableGateway
	dir     string
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
	dir := privateRuntimeDir(t)
	gateway, err := OpenDurableGateway(dir, commands)
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
		pool:    pool,
		adapter: adapter,
		gateway: gateway,
		dir:     dir,
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
	want := []string{
		"command_disposition",
		"agent_start", "message_end", "message_start", "message_end",
		"agent_end",
	}
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
	err := f.gateway.AppendProjectedEvents(f.ctx, f.pa, []ProjectedEvent{
		{Event: json.RawMessage(`{"type":"agent_start"}`)},
	})
	if !errors.Is(err, errProjectedWriteBlocked) {
		t.Fatalf("append under live runtime: %v", err)
	}
}

// userMessageIDFromCommandID must reproduce the browser's
// userMessageIdFromCommandId byte-for-byte — these vectors were generated by
// running that function (apps/web/src/agent/user-message-id.ts) directly.
func TestUserMessageIDFromCommandIDMatchesBrowser(t *testing.T) {
	vectors := map[string]string{
		// Anchored to the pre-existing Rust-namespace contract vector in
		// apps/web/src/agent/store.test.ts — Go, web, and legacy Rust must
		// all derive the same id.
		"00000000-0000-4000-8000-000000000001": "b508ee8b-fa35-59b0-8772-6f75ba135990",
		"018f3e2a-7c4b-7f61-9a2d-1e4f5a6b7c8d": "092bd7cf-c42b-548b-8e8a-2282b0000ecc",
		"00000000-0000-0000-0000-000000000000": "4f43c9b3-d525-50a0-a28a-620d24108dfb",
		"ffffffff-ffff-ffff-ffff-ffffffffffff": "13d107d8-1452-5d30-906f-cd13bedc31b5",
		"01997abc-1234-7cde-8f01-23456789abcd": "5e4a2277-2669-5694-b9fd-b9b67a75601a",
	}
	for commandID, want := range vectors {
		got, err := userMessageIDFromCommandID(commandID)
		if err != nil {
			t.Fatalf("command %s: %v", commandID, err)
		}
		if got != want {
			t.Fatalf("command %s: got %s, want %s", commandID, got, want)
		}
	}
}

// The projected user message must carry the canonical id the browser derives
// from the accepted command — otherwise the sent entry doubles on render.
func TestCoreDirectChatUserMessageHasCanonicalID(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	env := f.sendMessage(t, "key-1", "canonical id")
	in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID)
	if err != nil {
		t.Fatalf("input: %v", err)
	}
	f.runHostTurn(t, []agentstate.EventInput{
		f.inputReceived(in, 1),
		{Kind: "assistant_message", Payload: map[string]any{"text": "ok", "round": 0}},
	}, agentstate.CommitRequest{Outcome: "complete"})
	f.sweep(t)

	want, err := userMessageIDFromCommandID(env.CommandID)
	if err != nil {
		t.Fatalf("canonical id: %v", err)
	}
	var got string
	for _, e := range durableEvents(t, f.gateway, f.pa) {
		var ev struct {
			Type      string `json:"type"`
			MessageID string `json:"message_id"`
			Message   struct {
				Role string `json:"role"`
			} `json:"message"`
		}
		if json.Unmarshal(e.Event, &ev) == nil && ev.Type == "message_end" && ev.Message.Role == "user" {
			got = ev.MessageID
		}
	}
	if got != want {
		t.Fatalf("projected user message_id %q, canonical %q", got, want)
	}
}

// A run must open at first work and stay open across an approval park; the
// resumed attempt's events land in the same run, and agent_end arrives only
// once the input is terminally done — never while the approval is pending,
// where the reducer would drop the actionable prompt.
func TestCoreDirectChatRunEnvelopeAcrossApproval(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	env := f.sendMessage(t, "key-1", "note this")
	in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID)
	if err != nil {
		t.Fatalf("input: %v", err)
	}

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
	if err != nil || approval == nil {
		t.Fatalf("claim gated op: %v approval %v", err, approval)
	}
	// The real host journals only the approval request when a turn parks.
	if _, err := f.core.CommitTurn(f.ctx, f.pa, loaded.Turn.TurnID, lease.Generation,
		agentstate.CommitRequest{
			Outcome: "await",
			Events: []agentstate.EventInput{
				{Kind: "approval_requested", Payload: map[string]any{"tool": "journal.note", "call_id": "call-1", "route": "elevated", "approval_id": approval.ApprovalID, "request": map[string]any{"text": "x"}}},
			},
		}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	if err := f.core.ReleaseWriter(f.ctx, f.pa, "test-host", lease.Generation); err != nil {
		t.Fatalf("release: %v", err)
	}

	f.sweep(t)
	types := eventTypes(durableEvents(t, f.gateway, f.pa))
	if !containsEvent(types, "agent_start") || !containsEvent(types, "approval_requested") {
		t.Fatalf("parked turn projection missing run/approval: %v", types)
	}
	if containsEvent(types, "agent_end") {
		t.Fatalf("agent_end emitted while the approval is pending: %v", types)
	}
	if !f.gateway.IsApprovalPending(f.pa, approval.ApprovalID) {
		t.Fatal("approval not pending during park")
	}

	// The Human decides; the input requeues and a fresh turn resumes it,
	// journaling the whole turn once — the durable truth the projection must
	// render inside the still-open run.
	decision := fmt.Sprintf(`{"type":"approval_decision","request_id":%q,"decision":{"type":"approve_once"}}`, approval.ApprovalID)
	if _, err := f.adapter.Append(f.ctx, f.provenance(), "key-2", json.RawMessage(decision)); err != nil {
		t.Fatalf("approval_decision command: %v", err)
	}
	in2, _, err := f.core.GetInput(f.ctx, f.pa, in.InputID)
	if err != nil {
		t.Fatalf("input after decision: %v", err)
	}
	f.runHostTurn(t, []agentstate.EventInput{
		f.inputReceived(in2, 2),
		{Kind: "tool_call", Payload: map[string]any{"tool": "journal.note", "call_id": "call-1", "request": map[string]any{"text": "x"}, "route": "elevated"}},
		{Kind: "tool_result", Payload: map[string]any{"tool": "journal.note", "call_id": "call-1", "response": map[string]any{"noted": true}}},
		{Kind: "assistant_message", Payload: map[string]any{"text": "noted", "round": 1}},
	}, agentstate.CommitRequest{Outcome: "complete"})

	f.sweep(t)
	events := durableEvents(t, f.gateway, f.pa)
	types = eventTypes(events)
	if n := countEvent(types, "agent_start"); n != 1 {
		t.Fatalf("agent_start count %d, want 1: %v", n, types)
	}
	if n := countEvent(types, "agent_end"); n != 1 {
		t.Fatalf("agent_end count %d, want 1: %v", n, types)
	}
	if types[len(types)-1] != "agent_end" {
		t.Fatalf("run did not close last: %v", types)
	}
	if !containsEvent(types, "tool_execution_start") || !containsEvent(types, "tool_execution_end") {
		t.Fatalf("resumed tool events not projected: %v", types)
	}
	if !containsEvent(types, "approval_resolved") {
		t.Fatalf("approval_resolved missing: %v", types)
	}
	if texts := messageTexts(events, "assistant"); len(texts) != 1 || texts[0] != "noted" {
		t.Fatalf("assistant messages: %v", texts)
	}
}

func containsEvent(types []string, want string) bool {
	for _, ty := range types {
		if ty == want {
			return true
		}
	}
	return false
}

func countEvent(types []string, want string) int {
	var n int
	for _, ty := range types {
		if ty == want {
			n++
		}
	}
	return n
}

// Two projector instances over the same stores — a restarted API overlapping
// its predecessor — must commit each fact once. The durable dedup index is
// the guard; the per-process seen map alone cannot be.
func TestCoreDirectChatConcurrentProjectorsCommitOnce(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	env := f.sendMessage(t, "key-1", "once only")
	in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID)
	if err != nil {
		t.Fatalf("input: %v", err)
	}
	f.runHostTurn(t, []agentstate.EventInput{
		f.inputReceived(in, 1),
		{Kind: "assistant_message", Payload: map[string]any{"text": "reply", "round": 0}},
	}, agentstate.CommitRequest{Outcome: "complete"})

	gateway2, err := OpenDurableGateway(f.dir, f.gateway.commands)
	if err != nil {
		t.Fatalf("second gateway: %v", err)
	}
	adapter2 := &CoreDirectChat{Core: f.core, Gateway: gateway2}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a := f.adapter
			g := f.gateway
			if i%2 == 1 {
				a, g = adapter2, gateway2
			}
			if err := a.syncPersona(f.ctx, f.pa); err != nil {
				errs <- err
				return
			}
			if _, err := g.EventCatchUp(f.ctx, f.pa, 0); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent sync: %v", err)
	}

	events := durableEvents(t, f.gateway, f.pa)
	types := eventTypes(events)
	if n := countEvent(types, "agent_start"); n != 1 {
		t.Fatalf("agent_start committed %d times across projectors", n)
	}
	if n := countEvent(types, "agent_end"); n != 1 {
		t.Fatalf("agent_end committed %d times across projectors", n)
	}
	if texts := messageTexts(events, "user"); len(texts) != 1 {
		t.Fatalf("user message committed %d times: %v", len(texts), texts)
	}
	if texts := messageTexts(events, "assistant"); len(texts) != 1 {
		t.Fatalf("assistant message committed %d times: %v", len(texts), texts)
	}
}

// Distinct commands with identical text are intentional separate facts —
// dedup must never merge them.
func TestCoreDirectChatSameTextCommandsAreDistinct(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	f.sendMessage(t, "key-1", "same words")
	f.sendMessage(t, "key-2", "same words")
	commands, err := f.gateway.commands.CatchUp(f.ctx, f.pa, 1)
	if err != nil || len(commands) != 2 {
		t.Fatalf("commands: %v %v", commands, err)
	}
	for i, env := range commands {
		in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID)
		if err != nil {
			t.Fatalf("input %d: %v", i, err)
		}
		f.runHostTurn(t, []agentstate.EventInput{
			f.inputReceived(in, 1),
			{Kind: "assistant_message", Payload: map[string]any{"text": fmt.Sprintf("reply %d", i), "round": 0}},
		}, agentstate.CommitRequest{Outcome: "complete"})
	}
	f.sweep(t)
	if texts := messageTexts(durableEvents(t, f.gateway, f.pa), "user"); len(texts) != 2 {
		t.Fatalf("same-text commands merged: %v", texts)
	}
}

// A persona whose projection fails must not be retried every poll: the sweep
// backs off per persona, and a later success clears the penalty.
func TestCoreDirectChatSweepBackoffBoundsFailures(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	// A closed pool makes every core read fail deterministically — the
	// persistent-failure case the finding reported (log line every poll).
	pool := testdb.Create(t)
	adapter := &CoreDirectChat{Core: agentstate.NewStore(pool), Gateway: f.gateway}
	pool.Close()
	adapter.notePersona(f.pa)
	adapter.sweep(f.ctx)
	st := adapter.personaState(f.pa)
	if st.failures != 1 || st.nextRetry.IsZero() {
		t.Fatalf("first failure did not back off: failures=%d next=%v", st.failures, st.nextRetry)
	}
	adapter.sweep(f.ctx)
	if st.failures != 1 {
		t.Fatalf("backoff did not skip the retry: failures=%d", st.failures)
	}
}

// ---------------------------------------------------------------------------
// Review-B regressions: stale/overlapping projectors, sidecar loss, backoff
// saturation, and reconciler receipts. Each reproduces a deterministic
// finding with real Postgres + real file-backed logs.
// ---------------------------------------------------------------------------

// newProjector is a second "API process": its own gateway over the same
// durable dir, so its only shared truth is the committed log itself.
func (f *coreDirectChatFixture) newProjector(t *testing.T) (*CoreDirectChat, *DurableGateway) {
	t.Helper()
	gw, err := OpenDurableGateway(f.dir, f.gateway.commands)
	if err != nil {
		t.Fatalf("second gateway: %v", err)
	}
	return &CoreDirectChat{Core: f.core, Gateway: gw, PollInterval: time.Millisecond}, gw
}

func (f *coreDirectChatFixture) sendOn(t *testing.T, a *CoreDirectChat, key, text string) CommandEnvelope {
	t.Helper()
	env, err := a.Append(f.ctx, f.provenance(), key,
		json.RawMessage(fmt.Sprintf(`{"type":"user_message","text":%q,"attachments":[]}`, text)))
	if err != nil {
		t.Fatalf("append %q: %v", text, err)
	}
	return env
}

func syncOn(t *testing.T, a *CoreDirectChat, pa string) {
	t.Helper()
	if err := a.syncPersona(context.Background(), pa); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

// hostComplete finishes the input's claimed turn with the ordinary event set
// (input_received, one tool round-trip, assistant reply), like the TS host.
func (f *coreDirectChatFixture) hostComplete(t *testing.T, inputID, reply string) {
	t.Helper()
	in, _, err := f.core.GetInput(f.ctx, f.pa, inputID)
	if err != nil {
		t.Fatalf("get input %s: %v", inputID, err)
	}
	f.runHostTurn(t, []agentstate.EventInput{
		f.inputReceived(in, 1),
		{Kind: "tool_call", Payload: map[string]any{"tool": "journal.note", "call_id": "call-" + in.InputID, "request": map[string]any{"text": reply}, "route": "normal"}},
		{Kind: "tool_result", Payload: map[string]any{"tool": "journal.note", "call_id": "call-" + in.InputID, "response": map[string]any{"noted": reply}}},
		{Kind: "assistant_message", Payload: map[string]any{"text": reply, "round": 1}},
	}, agentstate.CommitRequest{Outcome: "complete"})
}

// hostPark commits the input's turn as awaiting an elevated approval: the
// input stays 'waiting' until the approval resolves.
func (f *coreDirectChatFixture) hostPark(t *testing.T, inputID string) (turnID, approvalID string) {
	t.Helper()
	lease, err := f.core.AcquireWriter(f.ctx, f.pa, "test-host", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	loaded, err := f.core.LoadTurn(f.ctx, f.pa, lease.Generation, "", 100)
	if err != nil || loaded.Turn == nil {
		t.Fatalf("load: %v turn=%v", err, loaded.Turn)
	}
	if _, _, err := f.core.SavePlan(f.ctx, f.pa, loaded.Turn.TurnID, lease.Generation, 0,
		agentstate.Decision{Calls: []agentstate.PlanCall{{
			Tool: "journal.note", Route: "elevated", Request: map[string]any{"text": "x"},
		}}}); err != nil {
		t.Fatalf("plan: %v", err)
	}
	_, approval, _, err := f.core.ClaimOperation(f.ctx, f.pa, loaded.Turn.TurnID,
		lease.Generation, "op-"+inputID, "journal.note", 0, map[string]any{"text": "x"})
	if err != nil || approval == nil {
		t.Fatalf("claim: %v approval=%v", err, approval)
	}
	if _, err := f.core.CommitTurn(f.ctx, f.pa, loaded.Turn.TurnID, lease.Generation,
		agentstate.CommitRequest{
			Outcome: "await",
			Events: []agentstate.EventInput{
				{Kind: "tool_call", Payload: map[string]any{"tool": "journal.note", "call_id": "call-park", "request": map[string]any{"text": "x"}, "route": "elevated"}},
				{Kind: "approval_requested", Payload: map[string]any{"tool": "journal.note", "call_id": "call-park", "route": "elevated", "approval_id": approval.ApprovalID, "request": map[string]any{"text": "x"}}},
			},
		}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	if err := f.core.ReleaseWriter(f.ctx, f.pa, "test-host", lease.Generation); err != nil {
		t.Fatalf("release: %v", err)
	}
	return loaded.Turn.TurnID, approval.ApprovalID
}

// assertEnvelopeIntegrity walks the committed log once and requires every
// non-marker event to sit inside an open run — content after the last
// agent_end with no agent_start after it is the orphan tail F1 produced.
func assertEnvelopeIntegrity(t *testing.T, events []Envelope) {
	t.Helper()
	open := false
	starts, ends := 0, 0
	var orphans []string
	for _, e := range events {
		var head struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(e.Event, &head); err != nil {
			continue
		}
		seq := uint64(0)
		if e.Seq != nil {
			seq = *e.Seq
		}
		switch head.Type {
		case "agent_start":
			starts++
			open = true
		case "agent_end":
			ends++
			open = false
		case "command_disposition":
			// Receipts are not run content: they land inside whatever run
			// is open and are also valid outside one.
		default:
			if !open {
				orphans = append(orphans, fmt.Sprintf("%s@%d", head.Type, seq))
			}
		}
	}
	if open {
		t.Fatalf("log ends with an unclosed run")
	}
	if starts != ends {
		t.Fatalf("unbalanced run markers: %d starts %d ends", starts, ends)
	}
	if len(orphans) > 0 {
		t.Fatalf("content committed outside a run envelope: %v", orphans)
	}
}

// F1: a projector whose cached view predates a whole busy period used to mint
// a start marker under a stale index, have it dedup-suppressed, then commit
// content after the committed agent_end — orphaning tool/approval events the
// browser reducer drops. Marker need/identity now comes from committed log
// state under the event-file lock.
func TestCoreDirectChatStaleProjectorKeepsContentInsideRun(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a, gwA := f.adapter, f.gateway

	// Busy period 0: completed by A.
	env1 := f.sendOn(t, a, "k1", "first")
	f.hostComplete(t, "direct-chat:"+env1.CommandID, "reply-1")
	syncOn(t, a, f.pa)
	if n := countEvent(eventTypes(durableEvents(t, gwA, f.pa)), "agent_start"); n != 1 {
		t.Fatalf("starts after run0: %d", n)
	}

	// B freezes its view now — before busy period 1 exists.
	b, gwB := f.newProjector(t)
	if err := b.loadProjectionState(f.ctx, f.pa, b.personaState(f.pa)); err != nil {
		t.Fatalf("B load: %v", err)
	}

	// Busy period 1: A projects a parked turn (run open), then resolves and
	// completes it (run closed).
	env2 := f.sendOn(t, a, "k2", "second")
	_, approvalID := f.hostPark(t, "direct-chat:"+env2.CommandID)
	syncOn(t, a, f.pa)
	if _, err := f.core.ResolveApproval(f.ctx, f.pa, approvalID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d1", DecidedByKind: "human", DecidedByID: f.user,
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	f.hostComplete(t, "direct-chat:"+env2.CommandID, "reply-2")
	syncOn(t, a, f.pa)

	// Busy period 2 is projected only by the stale B.
	env3 := f.sendOn(t, b, "k3", "third")
	f.hostComplete(t, "direct-chat:"+env3.CommandID, "reply-3")
	syncOn(t, b, f.pa)

	events := durableEvents(t, gwB, f.pa)
	assertEnvelopeIntegrity(t, events)
	if texts := messageTexts(events, "assistant"); len(texts) != 3 {
		t.Fatalf("assistant messages: %v", texts)
	}
	if n := countEvent(eventTypes(events), "agent_start"); n != 3 {
		t.Fatalf("starts: %d", n)
	}
}

// F2: losing the .dedup sidecar must not forget explicit marker keys — the
// rebuild recovers them positionally from the committed log, so replayed
// emissions still dedup. A stale projector closing after index loss used to
// commit a duplicate agent_end.
func TestCoreDirectChatDedupIndexLossPreservesMarkerKeys(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a, gwA := f.adapter, f.gateway

	env1 := f.sendOn(t, a, "k1", "first")
	f.hostComplete(t, "direct-chat:"+env1.CommandID, "reply-1")
	syncOn(t, a, f.pa)

	env2 := f.sendOn(t, a, "k2", "second")
	_, approvalID := f.hostPark(t, "direct-chat:"+env2.CommandID)
	syncOn(t, a, f.pa)

	b, _ := f.newProjector(t)
	if err := b.loadProjectionState(f.ctx, f.pa, b.personaState(f.pa)); err != nil {
		t.Fatalf("B load: %v", err)
	}

	if _, err := f.core.ResolveApproval(f.ctx, f.pa, approvalID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d1", DecidedByKind: "human", DecidedByID: f.user,
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	f.hostComplete(t, "direct-chat:"+env2.CommandID, "reply-2")
	syncOn(t, a, f.pa)

	before := durableEvents(t, gwA, f.pa)
	if n := countEvent(eventTypes(before), "agent_end"); n != 2 {
		t.Fatalf("ends before index loss: %d", n)
	}

	if err := os.Remove(filepath.Join(f.dir, "events-"+safeFileID(f.pa)+".dedup")); err != nil {
		t.Fatalf("remove dedup index: %v", err)
	}

	// The stale projector sweeps again: its view says a run is open, but the
	// committed log says closed — and the rebuilt index must still know the
	// committed marker keys either way.
	syncOn(t, b, f.pa)
	syncOn(t, a, f.pa)

	after := durableEvents(t, gwA, f.pa)
	assertEnvelopeIntegrity(t, after)
	if len(after) != len(before) {
		t.Fatalf("index loss caused new commits: before=%d after=%d", len(before), len(after))
	}
}

// F3: the sweep backoff must saturate, not overflow, at arbitrarily large
// failure counts — and a success must clear the penalty entirely.
func TestCoreDirectChatBackoffSaturatesHugeFailureCounts(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	pool := testdb.Create(t)
	adapter := &CoreDirectChat{Core: agentstate.NewStore(pool), Gateway: f.gateway, PollInterval: 500 * time.Millisecond}
	pool.Close()
	adapter.notePersona(f.pa)
	st := adapter.personaState(f.pa)

	for _, failures := range []int{1, 5, 35, 36, 40, 100, 1 << 20} {
		st.failures = failures - 1 // the sweep increments before computing
		st.nextRetry = time.Time{}
		adapter.sweep(f.ctx)
		if st.failures != failures {
			t.Fatalf("failures=%d want %d", st.failures, failures)
		}
		delay := time.Until(st.nextRetry)
		if delay <= 0 || delay > maxSyncBackoff+time.Second {
			t.Fatalf("failures=%d produced delay %v", failures, delay)
		}
	}
}

// F4: a durable user_message whose dispatch never ran (crash between command
// commit and SubmitInput) is resubmitted by the reconciler — and now also
// receives its applied receipt, exactly once, within the process lifetime.
func TestCoreDirectChatResubmittedCommandGetsApplied(t *testing.T) {
	f := newCoreDirectChatFixture(t)

	env, _, err := f.gateway.commands.appendWithIdempotencyStatus(f.ctx, f.provenance(), "k-orphan",
		json.RawMessage(`{"type":"user_message","text":"orphaned","attachments":[]}`))
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	f.sweep(t)

	if _, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID); err != nil {
		t.Fatalf("reconciler did not submit the input: %v", err)
	}
	f.hostComplete(t, "direct-chat:"+env.CommandID, "recovered")
	f.sweep(t)
	f.sweep(t) // a replayed sweep must not mint a second receipt

	var statuses []string
	for _, e := range durableEvents(t, f.gateway, f.pa) {
		var ev struct {
			Type      string `json:"type"`
			CommandID string `json:"command_id"`
			Status    string `json:"status"`
		}
		if json.Unmarshal(e.Event, &ev) == nil && ev.Type == "command_disposition" && ev.CommandID == env.CommandID {
			statuses = append(statuses, ev.Status)
		}
	}
	if len(statuses) != 1 || statuses[0] != "applied" {
		t.Fatalf("resubmitted command receipts: %v", statuses)
	}
}

// A fresh projector over an in-flight approval must see the open run in the
// committed log — folded from event lines, not from any cached counter — and
// must neither duplicate the start nor close the run while the input waits.
func TestCoreDirectChatRestartMidApprovalKeepsCommittedRun(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a, gwA := f.adapter, f.gateway

	env1 := f.sendOn(t, a, "k1", "hold this")
	_, approvalID := f.hostPark(t, "direct-chat:"+env1.CommandID)
	syncOn(t, a, f.pa)
	if n := countEvent(eventTypes(durableEvents(t, gwA, f.pa)), "agent_start"); n != 1 {
		t.Fatalf("starts: %d", n)
	}

	// API restart: brand-new gateway + adapter over the same dir.
	b, gwB := f.newProjector(t)
	if err := gwB.EnsureAgentSessionStateRebuilt(f.ctx, f.pa); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if !gwB.IsApprovalPending(f.pa, approvalID) {
		t.Fatal("pending approval not rebuilt across restart")
	}
	syncOn(t, b, f.pa)
	types := eventTypes(durableEvents(t, gwB, f.pa))
	if n := countEvent(types, "agent_start"); n != 1 {
		t.Fatalf("restart emitted a second agent_start: %d", n)
	}
	if n := countEvent(types, "agent_end"); n != 0 {
		t.Fatalf("restart closed the open run while the approval waits: %d", n)
	}

	// The decision taken on the restarted process resolves the parked turn;
	// the run closes exactly once, after the completion lands.
	decision := fmt.Sprintf(`{"type":"approval_decision","request_id":%q,"decision":{"type":"approve_once"}}`, approvalID)
	if _, err := b.Append(f.ctx, f.provenance(), "k2", json.RawMessage(decision)); err != nil {
		t.Fatalf("decision: %v", err)
	}
	f.hostComplete(t, "direct-chat:"+env1.CommandID, "resumed reply")
	syncOn(t, b, f.pa)

	events := durableEvents(t, gwB, f.pa)
	assertEnvelopeIntegrity(t, events)
	types = eventTypes(events)
	if n := countEvent(types, "agent_start"); n != 1 || countEvent(types, "agent_end") != 1 {
		t.Fatalf("run markers across restart: %v", types)
	}
	if n := countEvent(types, "approval_resolved"); n != 1 {
		t.Fatalf("approval_resolved missing: %v", types)
	}
}

// A crash between the index fsync and the event write leaves a key for a
// line that never committed. The phantom must be truncated on load —
// honoring it would suppress the next run's agent_start and orphan every
// event inside it.
func TestCoreDirectChatPhantomIndexRecordDoesNotSuppress(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a, gwA := f.adapter, f.gateway

	env1 := f.sendOn(t, a, "k1", "first")
	f.hostComplete(t, "direct-chat:"+env1.CommandID, "reply-1")
	syncOn(t, a, f.pa)
	if n := countEvent(eventTypes(durableEvents(t, gwA, f.pa)), "agent_start"); n != 1 {
		t.Fatalf("starts: %d", n)
	}

	// Forge a phantom: the exact key the NEXT busy period's agent_start
	// will use, without its event line ever committing.
	indexPath := filepath.Join(f.dir, "events-"+safeFileID(f.pa)+".dedup")
	ix, err := os.OpenFile(indexPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	k := runMarkerKey(f.pa, "start", 1)
	if _, err := ix.Write(k[:]); err != nil {
		t.Fatalf("phantom write: %v", err)
	}
	if err := ix.Close(); err != nil {
		t.Fatalf("phantom close: %v", err)
	}

	env2 := f.sendOn(t, a, "k2", "second")
	f.hostComplete(t, "direct-chat:"+env2.CommandID, "reply-2")
	syncOn(t, a, f.pa)

	events := durableEvents(t, gwA, f.pa)
	assertEnvelopeIntegrity(t, events)
	if n := countEvent(eventTypes(events), "agent_start"); n != 2 {
		t.Fatalf("phantom suppressed the next run's agent_start: starts=%d", n)
	}
}

// A torn event tail (crash mid-line) heals on the next access: read paths
// repair the partial record under the event lock and serve the committed
// prefix, so the projector appends cleanly.
func TestCoreDirectChatTornEventTailHealsOnAppend(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a, gwA := f.adapter, f.gateway

	env1 := f.sendOn(t, a, "k1", "first")
	f.hostComplete(t, "direct-chat:"+env1.CommandID, "reply-1")
	syncOn(t, a, f.pa)

	eventsPath := filepath.Join(f.dir, "events-"+safeFileID(f.pa)+".jsonl")
	ef, err := os.OpenFile(eventsPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open events: %v", err)
	}
	if _, err := ef.WriteString(`{"seq":9,"event":{"type":"message_end"`); err != nil {
		t.Fatalf("torn write: %v", err)
	}
	if err := ef.Close(); err != nil {
		t.Fatalf("torn close: %v", err)
	}

	// The read path repairs the torn record under the event lock and serves
	// the committed prefix — never the partial bytes themselves.
	prefix, err := gwA.EventCatchUp(f.ctx, f.pa, 0)
	if err != nil {
		t.Fatalf("torn tail not repaired on read: %v", err)
	}
	if n := len(durableEvents(t, gwA, f.pa)); len(prefix) != n {
		t.Fatalf("catch-up after repair returned %d events, log holds %d", len(prefix), n)
	}
	assertEnvelopeIntegrity(t, prefix)

	env2 := f.sendOn(t, a, "k2", "second")
	f.hostComplete(t, "direct-chat:"+env2.CommandID, "reply-2")
	syncOn(t, a, f.pa)
	events := durableEvents(t, gwA, f.pa)
	assertEnvelopeIntegrity(t, events)
	if texts := messageTexts(events, "assistant"); len(texts) != 2 {
		t.Fatalf("assistant messages after torn-tail heal: %v", texts)
	}
}

// A projector whose in-memory receipt map predates the committed applied
// disposition re-applies the decision; the identical replay resolves
// cleanly in the core store, so no contradictory superseded receipt lands.
func TestCoreDirectChatStaleProjectorNoContradictoryReceipt(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a, gwA := f.adapter, f.gateway

	env1 := f.sendOn(t, a, "k1", "park me")
	_, approvalID := f.hostPark(t, "direct-chat:"+env1.CommandID)
	syncOn(t, a, f.pa)

	b, _ := f.newProjector(t)
	if err := b.loadProjectionState(f.ctx, f.pa, b.personaState(f.pa)); err != nil {
		t.Fatalf("B load: %v", err)
	}

	decision := fmt.Sprintf(`{"type":"approval_decision","request_id":%q,"decision":{"type":"approve_once"}}`, approvalID)
	denv, err := a.Append(f.ctx, f.provenance(), "k2", json.RawMessage(decision))
	if err != nil {
		t.Fatalf("decision: %v", err)
	}
	f.hostComplete(t, "direct-chat:"+env1.CommandID, "done")
	syncOn(t, a, f.pa)

	// Stale B reconciles the same command log.
	syncOn(t, b, f.pa)

	var statuses []string
	for _, e := range durableEvents(t, gwA, f.pa) {
		var ev struct {
			Type      string `json:"type"`
			CommandID string `json:"command_id"`
			Status    string `json:"status"`
		}
		if json.Unmarshal(e.Event, &ev) == nil && ev.Type == "command_disposition" && ev.CommandID == denv.CommandID {
			statuses = append(statuses, ev.Status)
		}
	}
	if len(statuses) != 1 || statuses[0] != "applied" {
		t.Fatalf("decision command has contradictory receipts: %v", statuses)
	}
}

// F271: a projector whose idleness read goes stale must not close a run
// another projector opened. closeRunIfIdle anchors the END to the event
// tail observed before the PG read; the parked-hook rendezvous reproduces
// the exact race window deterministically — no sleeps.
func TestCoreDirectChatStaleIdleEndRefusedKeepsApproval(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a, gwA := f.adapter, f.gateway

	// A sweeps once at idle so its persona state is loaded; nothing commits.
	syncOn(t, a, f.pa)

	// Park A inside closeRunIfIdle after LiveDirectChatInputs resolved to 0
	// and before the END append — the interleaving the review reproduced
	// with a sleep, here a real rendezvous.
	parked := make(chan struct{})
	release := make(chan struct{})
	a.idleReadHook = func() {
		close(parked)
		<-release
	}
	staleEnd := make(chan error, 1)
	go func() { staleEnd <- a.closeRunIfIdle(context.Background(), f.pa) }()
	select {
	case <-parked:
	case <-time.After(15 * time.Second):
		t.Fatal("closeRunIfIdle did not reach the post-read rendezvous")
	}

	// B (second projector: own gateway over the same dir and PG store)
	// projects a whole new busy period — run opens, approval parks, the
	// input stays 'waiting'.
	b, gwB := f.newProjector(t)
	env := f.sendOn(t, b, "k1", "hold this")
	_, approvalID := f.hostPark(t, "direct-chat:"+env.CommandID)
	syncOn(t, b, f.pa)

	mid := eventTypes(durableEvents(t, gwB, f.pa))
	if !containsEvent(mid, "agent_start") || !containsEvent(mid, "approval_requested") {
		t.Fatalf("busy period not projected by B: %v", mid)
	}
	if containsEvent(mid, "agent_end") {
		t.Fatalf("run closed before the stale request lands: %v", mid)
	}
	if live, err := f.core.LiveDirectChatInputs(f.ctx, f.pa); err != nil || live != 1 {
		t.Fatalf("parked input must be live: live=%d err=%v", live, err)
	}

	// Release A. Its END was decided on the pre-busy-period tail; under the
	// append lock the committed tail has moved, so it must be refused.
	close(release)
	if err := <-staleEnd; err != nil {
		t.Fatalf("stale closeRunIfIdle: %v", err)
	}
	a.idleReadHook = nil

	after := durableEvents(t, gwB, f.pa)
	types := eventTypes(after)
	if containsEvent(types, "agent_end") {
		t.Fatalf("stale END committed mid-busy-period: %v", types)
	}
	if !gwB.IsApprovalPending(f.pa, approvalID) {
		t.Fatal("approval must still be pending after the refused END")
	}

	// The pending approval resolves and the turn completes — the whole busy
	// period must land in ONE run, closed by a later genuine idle pass (A's
	// own next sweep, with a fresh mark).
	if _, err := f.core.ResolveApproval(f.ctx, f.pa, approvalID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d1", DecidedByKind: "human", DecidedByID: f.user,
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	f.hostComplete(t, "direct-chat:"+env.CommandID, "resumed reply")
	syncOn(t, a, f.pa)

	final := durableEvents(t, gwA, f.pa)
	assertEnvelopeIntegrity(t, final)
	types = eventTypes(final)
	if n := countEvent(types, "agent_start"); n != 1 {
		t.Fatalf("busy period must stay one run; types=%v", types)
	}
	if n := countEvent(types, "agent_end"); n != 1 || types[len(types)-1] != "agent_end" {
		t.Fatalf("genuine idle close must land exactly once at the tail; types=%v", types)
	}
	if n := countEvent(types, "approval_resolved"); n != 1 {
		t.Fatalf("approval_resolved missing: %v", types)
	}
	// A replayed sweep must not mint a second end.
	syncOn(t, a, f.pa)
	syncOn(t, b, f.pa)
	if n := countEvent(eventTypes(durableEvents(t, gwA, f.pa)), "agent_end"); n != 1 {
		t.Fatalf("replayed sweeps minted extra ends; n=%d", n)
	}
}

// Same staleness without an approval: the refused END must not split one
// logical busy period into two durable runs.
func TestCoreDirectChatStaleIdleEndDoesNotSplitBusyPeriod(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a := f.adapter
	syncOn(t, a, f.pa)

	parked := make(chan struct{})
	release := make(chan struct{})
	a.idleReadHook = func() {
		close(parked)
		<-release
	}
	staleEnd := make(chan error, 1)
	go func() { staleEnd <- a.closeRunIfIdle(context.Background(), f.pa) }()
	select {
	case <-parked:
	case <-time.After(15 * time.Second):
		t.Fatal("closeRunIfIdle did not reach the post-read rendezvous")
	}

	// B projects only the start of the busy period: an 'await' commit keeps
	// the input claimed while its user message is already on the wire.
	b, gwB := f.newProjector(t)
	env := f.sendOn(t, b, "k1", "first half")
	in, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+env.CommandID)
	if err != nil {
		t.Fatalf("get input: %v", err)
	}
	lease, err := f.core.AcquireWriter(f.ctx, f.pa, "test-host", time.Minute)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	loaded, err := f.core.LoadTurn(f.ctx, f.pa, lease.Generation, "", 100)
	if err != nil || loaded.Turn == nil {
		t.Fatalf("load: %v", err)
	}
	if _, err := f.core.CommitTurn(f.ctx, f.pa, loaded.Turn.TurnID, lease.Generation,
		agentstate.CommitRequest{
			Outcome: "await",
			Events:  []agentstate.EventInput{f.inputReceived(in, 1)},
		}); err != nil {
		t.Fatalf("await commit: %v", err)
	}
	if err := f.core.ReleaseWriter(f.ctx, f.pa, "test-host", lease.Generation); err != nil {
		t.Fatalf("release: %v", err)
	}
	syncOn(t, b, f.pa)

	if live, _ := f.core.LiveDirectChatInputs(f.ctx, f.pa); live != 1 {
		t.Fatalf("claimed input must be live: %d", live)
	}
	close(release)
	if err := <-staleEnd; err != nil {
		t.Fatalf("stale closeRunIfIdle: %v", err)
	}
	a.idleReadHook = nil

	types := eventTypes(durableEvents(t, gwB, f.pa))
	if containsEvent(types, "agent_end") {
		t.Fatalf("stale END split the busy period: %v", types)
	}

	// Finish the turn: the tail of the same busy period joins the same run.
	f.runHostTurn(t, []agentstate.EventInput{
		{Kind: "assistant_message", Payload: map[string]any{"text": "second half", "round": 0}},
	}, agentstate.CommitRequest{Outcome: "complete"})
	syncOn(t, b, f.pa)
	types = eventTypes(durableEvents(t, gwB, f.pa))
	if n := countEvent(types, "agent_start"); n != 1 {
		t.Fatalf("busy period must not split; types=%v", types)
	}
	if n := countEvent(types, "agent_end"); n != 1 {
		t.Fatalf("busy period must close exactly once; types=%v", types)
	}
}

// F-B2: a browser attached to one API process must see state another
// process committed. Guards fold the committed event tail under the event
// lock — at socket attach AND again inside the admission check — so an
// already-open socket on the non-committing process admits a decision for
// an approval it never projected. Under the old latch this socket stayed
// permanently blind until restart.
func TestCoreDirectChatCrossProcessGuardSeesCommittedApprovals(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	gwA := f.gateway            // A: the browser-facing process
	b, gwB := f.newProjector(t) // B: the projecting process
	serverA := &BrowserServer{Events: gwA}

	// A's socket attaches and rebuilds while nothing is pending.
	if err := gwA.EnsureAgentSessionStateRebuilt(f.ctx, f.pa); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	decisionHead := func(requestID string) browserCommandHead {
		return browserCommandHead{Type: "approval_decision", RequestID: requestID}
	}
	if _, reject := serverA.checkCommandState(f.ctx, f.pa, decisionHead("req-none")); !reject {
		t.Fatal("decision admitted with no approval pending")
	}

	// B commits a busy period that parks on an approval. A never re-attaches
	// and appends nothing — the stale local maps are the defect's baseline.
	env1 := f.sendOn(t, b, "k1", "hold this")
	_, approvalID := f.hostPark(t, "direct-chat:"+env1.CommandID)
	syncOn(t, b, f.pa)
	if !gwB.IsApprovalPending(f.pa, approvalID) {
		t.Fatal("committing gateway blind to its own approval")
	}

	// The already-open socket on A aborts/decides against committed state:
	// admission refreshes the durable tail under the event lock first.
	if reason, reject := serverA.checkCommandState(f.ctx, f.pa, browserCommandHead{Type: "abort"}); reject {
		t.Fatalf("abort rejected though B's run is committed open: %q", reason)
	}
	if reason, reject := serverA.checkCommandState(f.ctx, f.pa, decisionHead(approvalID)); reject {
		t.Fatalf("decision for B's committed approval rejected on A: %q", reason)
	}

	// A admits the decision for real; B's reconciler resolves the parked
	// turn and projects approval_resolved.
	decision := fmt.Sprintf(`{"type":"approval_decision","request_id":%q,"decision":{"type":"approve_once"}}`, approvalID)
	if _, err := f.adapter.Append(f.ctx, f.provenance(), "k2", json.RawMessage(decision)); err != nil {
		t.Fatalf("decision append: %v", err)
	}
	f.hostComplete(t, "direct-chat:"+env1.CommandID, "resumed")
	syncOn(t, b, f.pa)

	events := durableEvents(t, gwB, f.pa)
	assertEnvelopeIntegrity(t, events)
	if n := countEvent(eventTypes(events), "approval_resolved"); n != 1 {
		t.Fatalf("approval_resolved missing: %v", eventTypes(events))
	}

	// Resolution is visible to A as well: a replayed decision is rejected,
	// and the abort window closed with the run.
	if reason, reject := serverA.checkCommandState(f.ctx, f.pa, decisionHead(approvalID)); !reject || reason != RejectNotAllowed {
		t.Fatalf("replayed decision after resolve: reject=%v reason=%q", reject, reason)
	}
	if reason, reject := serverA.checkCommandState(f.ctx, f.pa, browserCommandHead{Type: "abort"}); !reject || reason != RejectNotAllowed {
		t.Fatalf("abort admitted after run closed: reject=%v reason=%q", reject, reason)
	}
}

// F-B3: an API restart against a torn event tail must recover. The fresh
// projector's first sweep reads the log before any append — the read path
// now repairs the partial record under the event lock, then projection
// resumes on the committed prefix without duplicating or losing events.
func TestCoreDirectChatFreshProjectorHealsTornTail(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a, gwA := f.adapter, f.gateway

	env1 := f.sendOn(t, a, "k1", "first")
	f.hostComplete(t, "direct-chat:"+env1.CommandID, "reply-1")
	syncOn(t, a, f.pa)
	committed := durableEvents(t, gwA, f.pa)

	// Crash mid-append: a partial final record with no terminating newline.
	eventsPath := filepath.Join(f.dir, "events-"+safeFileID(f.pa)+".jsonl")
	ef, err := os.OpenFile(eventsPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open events: %v", err)
	}
	if _, err := ef.WriteString(`{"seq":9,"event":{"type":"message_end"`); err != nil {
		t.Fatalf("torn write: %v", err)
	}
	if err := ef.Close(); err != nil {
		t.Fatalf("torn close: %v", err)
	}

	// Simulated API restart: fresh gateway + projector over the same dir.
	// Under the old contract every sweep failed in loadProjectionState.
	b, gwB := f.newProjector(t)
	syncOn(t, b, f.pa)

	// History serves the committed prefix — the torn bytes are gone.
	replay, err := gwB.EventCatchUp(f.ctx, f.pa, 0)
	if err != nil {
		t.Fatalf("post-repair catch-up: %v", err)
	}
	if len(replay) != len(committed) {
		t.Fatalf("repaired prefix holds %d events, want %d", len(replay), len(committed))
	}
	assertEnvelopeIntegrity(t, replay)

	// Projection resumes on the repaired tail: new work appends cleanly.
	env2 := f.sendOn(t, b, "k2", "second")
	f.hostComplete(t, "direct-chat:"+env2.CommandID, "reply-2")
	syncOn(t, b, f.pa)
	events := durableEvents(t, gwB, f.pa)
	assertEnvelopeIntegrity(t, events)
	if texts := messageTexts(events, "assistant"); len(texts) != 2 {
		t.Fatalf("assistant messages after fresh-projector repair: %v", texts)
	}
}

// A newline-terminated record that is not valid JSON is corruption, not a
// torn tail: no path may silently truncate it — reads keep failing closed.
func TestCoreDirectChatMalformedTailStillFailsClosed(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	a, gwA := f.adapter, f.gateway

	env1 := f.sendOn(t, a, "k1", "first")
	f.hostComplete(t, "direct-chat:"+env1.CommandID, "reply-1")
	syncOn(t, a, f.pa)

	eventsPath := filepath.Join(f.dir, "events-"+safeFileID(f.pa)+".jsonl")
	ef, err := os.OpenFile(eventsPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open events: %v", err)
	}
	if _, err := ef.WriteString("{\"seq\":9,\"event\":{bad\n"); err != nil {
		t.Fatalf("malformed write: %v", err)
	}
	if err := ef.Close(); err != nil {
		t.Fatalf("malformed close: %v", err)
	}

	if _, err := gwA.EventCatchUp(f.ctx, f.pa, 0); err == nil {
		t.Fatal("malformed complete record silently served")
	}
	b, _ := f.newProjector(t)
	if err := b.syncPersona(f.ctx, f.pa); err == nil {
		t.Fatal("fresh projector sweep succeeded on malformed tail")
	}
	// The record must still be there — repair must not eat real corruption.
	raw, err := os.ReadFile(eventsPath)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if !bytes.Contains(raw, []byte("bad")) {
		t.Fatal("malformed record was silently truncated")
	}
}

// TestCoreDirectChatGuardPathRejectsReplayRefusedRecords is the C-1
// regression: a committed-log record that passes JSON/envelope format but
// breaks a replay invariant (inner seq mismatch or absent, or a foreign
// persona) must fail refresh, guard folding, command admission, and any
// further append — with the valid prefix and the corrupt bytes preserved.
func TestCoreDirectChatGuardPathRejectsReplayRefusedRecords(t *testing.T) {
	foreign := pid7(t)
	approvalLine := func(outer, inner uint64, persona, requestID string) string {
		return fmt.Sprintf(
			`{"seq":%d,"event":{"audience":"direct_chat","seq":%d,"personality_agent_id":%q,"event":{"type":"approval_requested","request":{"id":%q,"tool_call_id":"c1","tool_name":"journal.note","action":{"reviewable":{"text":"x"}},"args_summary":{"text":"x"}}}}}`+"\n",
			outer, inner, persona, requestID)
	}
	cases := []struct {
		name      string
		line      func(nextSeq uint64, pa string) string
		requestID string // approval request id carried by the corrupt record, if any
		errSubstr string
	}{
		{
			name: "persona-mismatch",
			line: func(n uint64, pa string) string {
				return approvalLine(n, n, foreign, "req-foreign")
			},
			requestID: "req-foreign",
			errSubstr: "personality agent mismatch",
		},
		{
			name: "inner-seq-mismatch",
			line: func(n uint64, pa string) string {
				return approvalLine(n, n+7, pa, "req-misseq")
			},
			requestID: "req-misseq",
			errSubstr: "seq mismatch",
		},
		{
			// A volatile event type is the only nil-seq envelope format
			// validation lets through; durable types already refuse nil seq
			// at decode. Either way refresh must not fold the record.
			name: "inner-seq-nil",
			line: func(n uint64, pa string) string {
				return fmt.Sprintf(
					`{"seq":%d,"event":{"audience":"direct_chat","personality_agent_id":%q,"event":{"type":"error","message":"x"}}}`+"\n",
					n, pa)
			},
			errSubstr: "seq mismatch",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newCoreDirectChatFixture(t)
			a, gwA := f.adapter, f.gateway
			env := f.sendOn(t, a, "k1", "first")
			f.hostComplete(t, "direct-chat:"+env.CommandID, "reply-1")
			syncOn(t, a, f.pa)
			nextSeq := uint64(len(durableEvents(t, gwA, f.pa))) + 1

			eventsPath := filepath.Join(f.dir, "events-"+safeFileID(f.pa)+".jsonl")
			line := tc.line(nextSeq, f.pa)
			ef, err := os.OpenFile(eventsPath, os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				t.Fatalf("open events: %v", err)
			}
			if _, err := ef.WriteString(line); err != nil {
				t.Fatalf("corrupt write: %v", err)
			}
			if err := ef.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			rawBefore, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatalf("read events: %v", err)
			}

			// Replay fails closed on the injected record.
			if _, err := gwA.EventCatchUp(f.ctx, f.pa, 0); err == nil {
				t.Fatal("EventCatchUp served a record replay must refuse")
			}
			// The refresh/guard path fails the same record before folding it.
			refreshErr := gwA.RefreshDurableEventTail(f.ctx, f.pa)
			if refreshErr == nil {
				t.Fatal("refresh folded a record replay refuses")
			}
			if !strings.Contains(refreshErr.Error(), tc.errSubstr) {
				t.Fatalf("refresh error %q does not name the %s", refreshErr, tc.errSubstr)
			}
			if tc.requestID != "" {
				if gwA.IsApprovalPending(f.pa, tc.requestID) {
					t.Fatal("refused record folded into this persona's pending approvals")
				}
				server := &BrowserServer{Events: gwA}
				reason, reject := server.checkCommandState(f.ctx, f.pa,
					browserCommandHead{Type: "approval_decision", RequestID: tc.requestID})
				if !reject || reason != RejectUnavailable {
					t.Fatalf("admission must fail closed as unavailable; got reject=%v reason=%q", reject, reason)
				}
			}
			// A projector sweep must not append after the refused record.
			env2 := f.sendOn(t, a, "k2", "second")
			f.hostComplete(t, "direct-chat:"+env2.CommandID, "reply-2")
			syncErr := a.syncPersona(f.ctx, f.pa)
			if syncErr == nil {
				t.Fatal("projector appended after a record replay refuses")
			}
			if !strings.Contains(syncErr.Error(), tc.errSubstr) {
				t.Fatalf("sync error %q does not name the %s", syncErr, tc.errSubstr)
			}
			rawAfter, err := os.ReadFile(eventsPath)
			if err != nil {
				t.Fatalf("read events after: %v", err)
			}
			if !bytes.Equal(rawAfter, rawBefore) {
				t.Fatal("corrupt tail was rewritten or appended after")
			}
		})
	}
}

// A command sent after the secretary transferred is durable, synchronously
// refused with ErrPersonaTransferred, and closed by the reconciler with a
// terminal rejected/secretary_moved disposition — not retried forever, not
// silently lost, and not labelled with a generic reason that hides the move.
func TestCoreDirectChatTransferredPersonaRejectsMoved(t *testing.T) {
	f := newCoreDirectChatFixture(t)

	env := f.sendMessage(t, "key-before", "sent before the move")
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'transferred' WHERE persona_id = $1`,
		f.pa); err != nil {
		t.Fatalf("transfer persona: %v", err)
	}

	movedEnv, err := f.adapter.Append(f.ctx, f.provenance(), "key-after",
		json.RawMessage(`{"type":"user_message","text":"after the move","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaTransferred) ||
		!errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("append after transfer: %v", err)
	}
	if movedEnv.CommandID == "" || movedEnv.Seq != 2 {
		t.Fatalf("moved command lost its durable identity: %+v", movedEnv)
	}
	// No core input may be admitted for the transferred persona.
	if _, _, err := f.core.GetInput(f.ctx, f.pa, "direct-chat:"+movedEnv.CommandID); !errors.Is(err, agentstate.ErrInputNotFound) {
		t.Fatalf("transferred persona queued an input: %v", err)
	}

	f.sweep(t)
	events := durableEvents(t, f.gateway, f.pa)
	var dispositions []map[string]any
	for _, e := range events {
		var ev map[string]any
		if err := json.Unmarshal(e.Event, &ev); err != nil {
			continue
		}
		if ev["type"] == "command_disposition" {
			dispositions = append(dispositions, ev)
		}
	}
	if len(dispositions) != 2 {
		t.Fatalf("dispositions: %v", dispositions)
	}
	// The pre-move command's input was admitted while active — applied.
	if dispositions[0]["command_id"] != env.CommandID || dispositions[0]["status"] != "applied" {
		t.Fatalf("pre-move disposition: %v", dispositions[0])
	}
	if dispositions[1]["command_id"] != movedEnv.CommandID ||
		dispositions[1]["status"] != "rejected" ||
		dispositions[1]["reject_reason"] != string(RejectSecretaryMoved) {
		t.Fatalf("post-move disposition: %v", dispositions[1])
	}

	// Re-sweeping after restart must not append duplicates or retry.
	restarted := &CoreDirectChat{Core: f.core, Gateway: f.gateway}
	if err := restarted.syncPersona(f.ctx, f.pa); err != nil {
		t.Fatalf("resync after restart: %v", err)
	}
	if after := durableEvents(t, f.gateway, f.pa); len(after) != len(events) {
		t.Fatalf("resync appended %d duplicate events", len(after)-len(events))
	}
}

// A sealed persona mid-move is inactive but NOT moved: its rejected
// disposition must stay not_allowed, never claim the secretary is gone.
func TestCoreDirectChatSealedPersonaStaysNotAllowed(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`,
		f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	_, err := f.adapter.Append(f.ctx, f.provenance(), "key-sealed",
		json.RawMessage(`{"type":"user_message","text":"mid move","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) ||
		errors.Is(err, agentstate.ErrPersonaTransferred) {
		t.Fatalf("sealed append: %v", err)
	}
	f.sweep(t)
	var rejected map[string]any
	for _, e := range durableEvents(t, f.gateway, f.pa) {
		var ev map[string]any
		if json.Unmarshal(e.Event, &ev) == nil && ev["type"] == "command_disposition" {
			rejected = ev
		}
	}
	if rejected["status"] != "rejected" || rejected["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("sealed disposition: %v", rejected)
	}
}

func commandDispositions(t *testing.T, g *DurableGateway, pa string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, e := range durableEvents(t, g, pa) {
		var ev map[string]any
		if json.Unmarshal(e.Event, &ev) == nil && ev["type"] == "command_disposition" {
			out = append(out, ev)
		}
	}
	return out
}

// A command's first terminal disposition is final: a command rejected
// not_allowed while the persona was sealed keeps that receipt after the
// transfer completes and the projector restarts — no secretary_moved
// revision, no duplicate disposition. Regression for the defect where a
// restarted projector rescanned the command (commandSeq resets to 0) and
// appended a second, differently-reasoned receipt, which then broke
// CommandDispositionFor.
func TestCoreDirectChatDispositionStaysFinalAcrossTransferAndRestart(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	env, err := f.adapter.Append(f.ctx, f.provenance(), "sealed-key",
		json.RawMessage(`{"type":"user_message","text":"during seal","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("sealed append err=%v", err)
	}
	f.sweep(t)
	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 1 ||
		dispositions[0]["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("sealed dispositions: %v", dispositions)
	}

	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'transferred' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("transfer persona: %v", err)
	}
	// A restarted projector rebuilds its cursors from the durable log and
	// rescans every command; the committed receipt must stay the only one.
	restarted := &CoreDirectChat{Core: f.core, Gateway: f.gateway}
	if err := restarted.syncPersona(f.ctx, f.pa); err != nil {
		t.Fatalf("post-restart sweep: %v", err)
	}
	if err := restarted.syncPersona(f.ctx, f.pa); err != nil {
		t.Fatalf("second post-restart sweep: %v", err)
	}
	dispositions = commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 1 {
		t.Fatalf("restart appended a second disposition: %v", dispositions)
	}
	if dispositions[0]["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("terminal disposition was revised: %v", dispositions[0])
	}
	disposition, found, derr := f.gateway.CommandDispositionFor(f.ctx, env)
	if derr != nil || !found {
		t.Fatalf("CommandDispositionFor after restart: found=%v err=%v", found, derr)
	}
	var parsed map[string]any
	if err := json.Unmarshal(disposition, &parsed); err != nil ||
		parsed["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("lookup returned %s", disposition)
	}
}

// A sibling projector whose in-memory view predates the committed receipt
// (it loaded before the first projector's disposition landed) must still be
// refused by the command-scoped durable dedup identity under the event-file
// lock — even when its fresher authority read would classify the command
// differently.
func TestCoreDirectChatStaleProjectorCannotDoubleDispose(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	env, err := f.adapter.Append(f.ctx, f.provenance(), "stale-key",
		json.RawMessage(`{"type":"user_message","text":"stale view","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("sealed append err=%v", err)
	}

	// Sibling projector on its own gateway loads its projection state
	// before the first projector's receipt commits.
	gateway2, err := OpenDurableGateway(f.dir, f.gateway.commands)
	if err != nil {
		t.Fatalf("second gateway: %v", err)
	}
	sibling := &CoreDirectChat{Core: f.core, Gateway: gateway2}
	st2 := sibling.personaState(f.pa)
	if err := sibling.loadProjectionState(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling load: %v", err)
	}

	// First projector disposes the command while sealed; the transfer then
	// completes, so the sibling's stale state now computes secretary_moved.
	f.sweep(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'transferred' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("transfer persona: %v", err)
	}
	if err := sibling.reconcileCommands(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling reconcile: %v", err)
	}

	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 1 ||
		dispositions[0]["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("sibling projector committed a second disposition: %v", dispositions)
	}
	if _, found, derr := f.gateway.CommandDispositionFor(f.ctx, env); derr != nil || !found {
		t.Fatalf("CommandDispositionFor: found=%v err=%v", found, derr)
	}
}

// A restart must still reconcile commands that never got a terminal
// receipt: rescanning is how pending work survives a projector restart.
// Both shapes are covered — an admitted user_message earns "applied", and
// a durable abort-type command earns its rejected receipt exactly once.
func TestCoreDirectChatRestartStillReconcilesUndisposedCommands(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	env := f.sendMessage(t, "pending-key", "admitted before restart")
	abortEnv, err := f.adapter.Append(f.ctx, f.provenance(), "abort-key",
		json.RawMessage(`{"type":"abort"}`))
	if err != nil {
		t.Fatalf("append abort: %v", err)
	}
	// No sweep before the "restart": neither command has a receipt yet.
	restarted := &CoreDirectChat{Core: f.core, Gateway: f.gateway}
	if err := restarted.syncPersona(f.ctx, f.pa); err != nil {
		t.Fatalf("post-restart sweep: %v", err)
	}
	if err := restarted.syncPersona(f.ctx, f.pa); err != nil {
		t.Fatalf("second post-restart sweep: %v", err)
	}

	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 2 {
		t.Fatalf("expected exactly one receipt per command, got %v", dispositions)
	}
	byID := map[string]map[string]any{}
	for _, d := range dispositions {
		byID[d["command_id"].(string)] = d
	}
	if byID[env.CommandID]["status"] != "applied" {
		t.Fatalf("admitted command disposition: %v", byID[env.CommandID])
	}
	if byID[abortEnv.CommandID]["status"] != "rejected" ||
		byID[abortEnv.CommandID]["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("abort disposition: %v", byID[abortEnv.CommandID])
	}
	if _, found, derr := f.gateway.CommandDispositionFor(f.ctx, abortEnv); derr != nil || !found {
		t.Fatalf("abort CommandDispositionFor: found=%v err=%v", found, derr)
	}
}

func dedupIndexPath(f *coreDirectChatFixture) string {
	return filepath.Join(f.dir, "events-"+safeFileID(f.pa)+".dedup")
}

func readDedupIndex(t *testing.T, f *coreDirectChatFixture) []byte {
	t.Helper()
	raw, err := os.ReadFile(dedupIndexPath(f))
	if err != nil {
		t.Fatalf("read dedup index: %v", err)
	}
	return raw
}

// A sibling projector whose in-memory view predates the committed receipt
// must still be refused after the dedup index is lost wholesale: index
// recovery has to reproduce the receipt's command-scoped key from the
// stored command_id — hashing the stored content cannot — or the stale
// sibling commits a second terminal disposition and CommandDispositionFor
// breaks. Regression for the recovery-gap variant of the duplicate-
// disposition defect.
func TestCoreDirectChatLostDedupIndexStaleProjectorCannotDoubleDispose(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	env, err := f.adapter.Append(f.ctx, f.provenance(), "lost-index-key",
		json.RawMessage(`{"type":"user_message","text":"stale view","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("sealed append err=%v", err)
	}

	gateway2, err := OpenDurableGateway(f.dir, f.gateway.commands)
	if err != nil {
		t.Fatalf("second gateway: %v", err)
	}
	sibling := &CoreDirectChat{Core: f.core, Gateway: gateway2}
	st2 := sibling.personaState(f.pa)
	if err := sibling.loadProjectionState(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling load: %v", err)
	}

	f.sweep(t)
	if err := os.Remove(dedupIndexPath(f)); err != nil {
		t.Fatalf("remove dedup index: %v", err)
	}
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'transferred' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("transfer persona: %v", err)
	}
	if err := sibling.reconcileCommands(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling reconcile: %v", err)
	}

	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 1 ||
		dispositions[0]["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("lost-index stale projector committed a second disposition: %v", dispositions)
	}
	if _, found, derr := f.gateway.CommandDispositionFor(f.ctx, env); derr != nil || !found {
		t.Fatalf("CommandDispositionFor: found=%v err=%v", found, derr)
	}
	// The rebuilt index must carry the command-scoped preimage, not the
	// content hash that could never refuse this append again.
	idx := readDedupIndex(t, f)
	want := commandDispositionKey(env.CommandID)
	if len(idx)%sha256.Size != 0 || len(idx) == 0 ||
		!bytes.Equal(idx[len(idx)-sha256.Size:], want[:]) {
		t.Fatalf("rebuilt index does not end with the command-scoped key: %x", idx)
	}
}

// The lost-index gap does not need an authority change: a stale sibling
// recomputing the *same* reason emits byte-identical content under the
// command-scoped key, which a content-hash rebuild cannot match — so the
// duplicate commits even for identical receipts.
func TestCoreDirectChatLostDedupIndexSameReasonReplayRefused(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	env, err := f.adapter.Append(f.ctx, f.provenance(), "same-reason-key",
		json.RawMessage(`{"type":"user_message","text":"same reason","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("sealed append err=%v", err)
	}

	gateway2, err := OpenDurableGateway(f.dir, f.gateway.commands)
	if err != nil {
		t.Fatalf("second gateway: %v", err)
	}
	sibling := &CoreDirectChat{Core: f.core, Gateway: gateway2}
	st2 := sibling.personaState(f.pa)
	if err := sibling.loadProjectionState(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling load: %v", err)
	}

	f.sweep(t)
	if err := os.Remove(dedupIndexPath(f)); err != nil {
		t.Fatalf("remove dedup index: %v", err)
	}
	// Authority stays sealed: the sibling recomputes the identical
	// not_allowed receipt for the same command.
	if err := sibling.reconcileCommands(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling reconcile: %v", err)
	}

	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 1 ||
		dispositions[0]["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("same-reason replay committed a second disposition: %v", dispositions)
	}
	if _, found, derr := f.gateway.CommandDispositionFor(f.ctx, env); derr != nil || !found {
		t.Fatalf("CommandDispositionFor: found=%v err=%v", found, derr)
	}
}

// A torn index tail — a preimage lost mid-write — must re-cover the
// committed disposition with its command-scoped key, and the index must
// stay preimage-aligned with the committed lines it covers.
func TestCoreDirectChatTornDedupIndexStaleProjectorCannotDoubleDispose(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	env1, err := f.adapter.Append(f.ctx, f.provenance(), "torn-one",
		json.RawMessage(`{"type":"user_message","text":"first","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("sealed append 1 err=%v", err)
	}
	env2, err := f.adapter.Append(f.ctx, f.provenance(), "torn-two",
		json.RawMessage(`{"type":"user_message","text":"second","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("sealed append 2 err=%v", err)
	}

	// The sibling loads before either receipt commits, so its in-memory
	// view is stale and only the durable dedup identity can refuse it.
	gateway2, err := OpenDurableGateway(f.dir, f.gateway.commands)
	if err != nil {
		t.Fatalf("second gateway: %v", err)
	}
	sibling := &CoreDirectChat{Core: f.core, Gateway: gateway2}
	st2 := sibling.personaState(f.pa)
	if err := sibling.loadProjectionState(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling load: %v", err)
	}

	f.sweep(t)
	f.sweep(t)

	// Tear the tail: keep the first preimage, drop the second plus a
	// half-written record — the documented torn-write shape.
	eventCount := int64(len(durableEvents(t, f.gateway, f.pa)))
	if eventCount != 2 {
		t.Fatalf("expected 2 committed events, got %d", eventCount)
	}
	idx := readDedupIndex(t, f)
	if int64(len(idx)) != eventCount*sha256.Size {
		t.Fatalf("index preimage count %d does not match %d events", len(idx)/sha256.Size, eventCount)
	}
	torn := int64(sha256.Size) + 10
	if err := os.Truncate(dedupIndexPath(f), torn); err != nil {
		t.Fatalf("truncate dedup index: %v", err)
	}

	if err := sibling.reconcileCommands(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling reconcile: %v", err)
	}

	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 2 {
		t.Fatalf("torn-index stale projector committed extra dispositions: %v", dispositions)
	}
	for _, env := range []CommandEnvelope{env1, env2} {
		if _, found, derr := f.gateway.CommandDispositionFor(f.ctx, env); derr != nil || !found {
			t.Fatalf("CommandDispositionFor %s: found=%v err=%v", env.CommandID, found, derr)
		}
	}
	// The torn preimage must be re-covered by the command-scoped key and
	// the index brought back to exactly one record per committed line.
	idx = readDedupIndex(t, f)
	if int64(len(idx)) != eventCount*sha256.Size {
		t.Fatalf("index preimage count %d no longer matches %d events", len(idx)/sha256.Size, eventCount)
	}
	want := commandDispositionKey(env2.CommandID)
	if !bytes.Equal(idx[len(idx)-sha256.Size:], want[:]) {
		t.Fatalf("re-covered preimage is not the command-scoped key: %x", idx[len(idx)-sha256.Size:])
	}
}

// An intact index whose disposition record was written under the legacy
// content-hash identity (pre-command-scoped builds) must still refuse a
// stale sibling: recovery reproduces the command-scoped key from the
// committed line even when every line already has an index record.
func TestCoreDirectChatLegacyKeyedDispositionStillRefusesStaleProjector(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	env, err := f.adapter.Append(f.ctx, f.provenance(), "legacy-key",
		json.RawMessage(`{"type":"user_message","text":"legacy","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("sealed append err=%v", err)
	}

	gateway2, err := OpenDurableGateway(f.dir, f.gateway.commands)
	if err != nil {
		t.Fatalf("second gateway: %v", err)
	}
	sibling := &CoreDirectChat{Core: f.core, Gateway: gateway2}
	st2 := sibling.personaState(f.pa)
	if err := sibling.loadProjectionState(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling load: %v", err)
	}

	f.sweep(t)
	events := durableEvents(t, f.gateway, f.pa)
	if len(events) != 1 {
		t.Fatalf("expected 1 committed event, got %d", len(events))
	}
	// Rewrite the sole index record as a legacy content-hash preimage:
	// the shape every pre-command-scoped build left on disk.
	legacy := sha256.Sum256(events[0].Event)
	if err := os.WriteFile(dedupIndexPath(f), legacy[:], 0o600); err != nil {
		t.Fatalf("rewrite dedup index: %v", err)
	}
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'transferred' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("transfer persona: %v", err)
	}
	if err := sibling.reconcileCommands(f.ctx, f.pa, st2); err != nil {
		t.Fatalf("sibling reconcile: %v", err)
	}

	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 1 ||
		dispositions[0]["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("legacy-keyed index allowed a second disposition: %v", dispositions)
	}
	if _, found, derr := f.gateway.CommandDispositionFor(f.ctx, env); derr != nil || !found {
		t.Fatalf("CommandDispositionFor: found=%v err=%v", found, derr)
	}
}

// A preimage of an event that never committed must be truncated, not
// trusted: a phantom command-scoped key in the index cannot suppress the
// real disposition when the command later reaches a projector.
func TestCoreDirectChatPhantomDedupKeyDoesNotSuppressDisposition(t *testing.T) {
	f := newCoreDirectChatFixture(t)
	if _, err := f.pool.Exec(f.ctx,
		`UPDATE core_personas SET authority = 'sealed' WHERE persona_id = $1`, f.pa); err != nil {
		t.Fatalf("seal persona: %v", err)
	}
	env1, err := f.adapter.Append(f.ctx, f.provenance(), "phantom-committed",
		json.RawMessage(`{"type":"user_message","text":"committed","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("sealed append 1 err=%v", err)
	}
	f.sweep(t)

	env2, err := f.adapter.Append(f.ctx, f.provenance(), "phantom-pending",
		json.RawMessage(`{"type":"user_message","text":"pending","attachments":[]}`))
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("sealed append 2 err=%v", err)
	}
	// Forge the torn-write shape where a preimage outlived its event:
	// the index holds the pending command's disposition key although no
	// such event ever committed.
	phantom := commandDispositionKey(env2.CommandID)
	ix, err := os.OpenFile(dedupIndexPath(f), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open dedup index: %v", err)
	}
	if _, err := ix.Write(phantom[:]); err != nil {
		ix.Close()
		t.Fatalf("write phantom key: %v", err)
	}
	ix.Close()

	f.sweep(t)
	dispositions := commandDispositions(t, f.gateway, f.pa)
	if len(dispositions) != 2 {
		t.Fatalf("phantom key suppressed a real disposition: %v", dispositions)
	}
	byID := map[string]map[string]any{}
	for _, d := range dispositions {
		byID[d["command_id"].(string)] = d
	}
	for _, env := range []CommandEnvelope{env1, env2} {
		if _, found, derr := f.gateway.CommandDispositionFor(f.ctx, env); derr != nil || !found {
			t.Fatalf("CommandDispositionFor %s: found=%v err=%v", env.CommandID, found, derr)
		}
	}
	if byID[env2.CommandID]["reject_reason"] != string(RejectNotAllowed) {
		t.Fatalf("pending command disposition: %v", byID[env2.CommandID])
	}
}
