package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/websocket"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func attentionTestProvenance() IncomingProvenance {
	const id = "018f47a2-9b3c-7def-8abc-0123456789ab"
	return IncomingProvenance{Version: 2, TenantID: "test-tenant", PersonalityAgentID: id,
		Actor: ProvenanceActor{Kind: "human", PrincipalID: "human-author", DisplayName: "Actual author"},
		Source: ProvenanceSource{Surface: "messaging", EventID: id, Kind: "messaging_mention", WorkspaceID: id, InstallationID: id, AuthorityEpoch: 1,
			Place: &ProvenancePlace{ID: id, Kind: "channel", Name: "Shared place"}, MessageID: id, MessageRevision: 1, MessageSeq: 1, OccurredAt: "2026-09-08T12:00:00Z"}}
}

func TestAttentionCommandSeparatesSourceFromBrowserAuthority(t *testing.T) {
	p := attentionTestProvenance()
	command := json.RawMessage(`{"type":"external_event","content":"Original text"}`)
	if err := validateIncomingCommand(p, command); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCommand(command); err == nil {
		t.Fatal("browser accepted internal external event")
	}
	if err := validateIncomingCommand(testDirectChatProvenance(p.PersonalityAgentID), command); err == nil {
		t.Fatal("direct chat disguised an external event")
	}
	if err := validateIncomingCommand(p, json.RawMessage(`{"type":"approval_decision","request_id":"x","decision":{"type":"approve"}}`)); err == nil {
		t.Fatal("external source accepted approval authority")
	}
	for name, mutate := range map[string]func(*IncomingProvenance){
		"unknown kind":      func(p *IncomingProvenance) { p.Source.Kind = "ambient" },
		"fake version":      func(p *IncomingProvenance) { p.Version = 1 },
		"missing place":     func(p *IncomingProvenance) { p.Source.Place = nil },
		"invented reminder": func(p *IncomingProvenance) { p.Source.Kind = "reply_later_due" },
	} {
		t.Run(name, func(t *testing.T) {
			p := attentionTestProvenance()
			mutate(&p)
			if p.Validate() == nil {
				t.Fatal("invalid provenance accepted")
			}
		})
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var decoded IncomingProvenance
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !p.Equal(decoded) {
		t.Fatal("value-identical source changed across serialization")
	}
}

func TestAttentionLookupReconcilesRestartWithoutRuntime(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	p := attentionTestProvenance()
	command := json.RawMessage(`{"type":"external_event","content":"Original text"}`)
	const key = "attention:owned:key"
	if _, found, err := s.Lookup(ctx, p, key, command); err != nil || found {
		t.Fatalf("initial lookup: %v %v", found, err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "commands-*.jsonl"))
	if err != nil || len(files) != 0 {
		t.Fatalf("lookup created command log: %v %v", files, err)
	}
	first, err := s.Append(ctx, p, key, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	gateway, err := OpenDurableGateway(privateRuntimeDir(t), s)
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := gateway.LookupAdmission(ctx, p, key, command)
	if err != nil || !found || got.CommandID != first.CommandID || got.Seq != first.Seq {
		t.Fatalf("restart lookup: %+v %v %v", got, found, err)
	}
	if _, err := gateway.Append(ctx, p, key, command); err == nil {
		t.Fatal("absent runtime accepted admission path")
	}
	changed := p
	changed.Actor.PrincipalID = "someone-else"
	if _, _, err := s.Lookup(ctx, changed, key, command); !errors.Is(err, errIdempotencyConflict) {
		t.Fatalf("changed actor: %v", err)
	}
	changed = p
	place := *p.Source.Place
	place.Name = "other"
	changed.Source.Place = &place
	if _, _, err := s.Lookup(ctx, changed, key, command); !errors.Is(err, errIdempotencyConflict) {
		t.Fatalf("changed place: %v", err)
	}
	if _, _, err := s.Lookup(ctx, p, key, json.RawMessage(`{"type":"external_event","content":"Changed"}`)); !errors.Is(err, errIdempotencyConflict) {
		t.Fatalf("changed body: %v", err)
	}
	replay, err := s.Append(ctx, got.Provenance, key, command)
	if err != nil || replay.Seq != first.Seq {
		t.Fatalf("repeat: %+v %v", replay, err)
	}
}

func TestAttentionBrowserShowsAllExecutionContextsAcrossReload(t *testing.T) {
	ctx := context.Background()
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, attentionTestProvenance().PersonalityAgentID)
	for i, audience := range []OutputAudience{AudienceSecretary, AudienceSecretary, AudienceDirectChat} {
		seq := uint64(i + 1)
		if err := gateway.Receive(ctx, claims, Envelope{Seq: &seq, PersonalityAgentID: claims.PersonalityAgentID, Audience: audience, Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	server := &BrowserServer{Events: gateway}
	var frames []any
	next, err := server.browserDurableCatchUp(ctx, claims.PersonalityAgentID, 0, func(frame any) error { frames = append(frames, frame); return nil })
	if err != nil || next != 3 || len(frames) != 3 {
		t.Fatalf("catchup %d %v %+v", next, err, frames)
	}
	for i, raw := range frames {
		frame, ok := raw.(browserEventFrame)
		if !ok || *frame.Envelope.Seq != uint64(i+1) {
			t.Fatalf("noncontiguous frame: %+v", raw)
		}
	}
	frames = nil
	next, err = server.browserDurableCatchUp(ctx, claims.PersonalityAgentID, 2, func(frame any) error { frames = append(frames, frame); return nil })
	if err != nil || next != 3 || len(frames) != 1 {
		t.Fatalf("reload %d %v %+v", next, err, frames)
	}
}

func TestAttentionContextRequiredAndSecretaryProjectionAllowed(t *testing.T) {
	p := attentionTestProvenance()
	seq := uint64(1)
	envelope := Envelope{Seq: &seq, PersonalityAgentID: p.PersonalityAgentID, Event: json.RawMessage(`{"type":"agent_start"}`)}
	if validateEnvelope(envelope) == nil {
		t.Fatal("missing audience accepted")
	}
	envelope.Audience = AudienceSecretary
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Envelope
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Audience != AudienceSecretary {
		t.Fatal("audience lost")
	}
	projected, err := projectBrowserEvent(decoded)
	if err != nil || projected.Audience != AudienceSecretary {
		t.Fatalf("secretary context projection: %v", err)
	}

}

func TestAttentionVolatileMergeCannotCrossAudience(t *testing.T) {
	a := Envelope{PersonalityAgentID: attentionTestProvenance().PersonalityAgentID, Audience: AudienceDirectChat, Event: json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"visible"}}`)}
	b := a
	b.Audience = AudienceSecretary
	if _, ok := mergeBrowserDelta(a, b); ok {
		t.Fatal("private text merged into public delta")
	}
	b.Audience = AudienceDirectChat
	if _, ok := mergeBrowserDelta(a, b); !ok {
		t.Fatal("same-audience coalescing regressed")
	}
}

type attentionTestSpawner struct {
	gateway  *DurableGateway
	ready    bool
	released bool
	held     bool
}

func (s *attentionTestSpawner) EnsureRunning(ctx context.Context, pa string) error {
	var receipt *string
	if s.ready {
		value := "attention-ready"
		receipt = &value
	}
	return s.gateway.PublishRuntimeState(pa, 1, receipt)
}
func (*attentionTestSpawner) Touch(string) {}
func (s *attentionTestSpawner) HoldAdmission(string) (func(), error) {
	s.held = true
	return func() { s.released = true }, nil
}

func TestAttentionPrepareHoldsReadyRuntimeAndReleasesOnCancellation(t *testing.T) {
	for _, ready := range []bool{true, false} {
		gateway := openRuntimeGateway(t)
		spawner := &attentionTestSpawner{gateway: gateway, ready: ready}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		release, err := gateway.PrepareAttention(ctx, spawner, attentionTestProvenance().PersonalityAgentID)
		cancel()
		if !spawner.held {
			t.Fatal("runtime was not held during readiness wait")
		}
		if ready {
			if err != nil || release == nil || spawner.released {
				t.Fatalf("ready: %v", err)
			}
			p := attentionTestProvenance()
			if _, err := gateway.Append(context.Background(), p, "attention-ready", json.RawMessage(`{"type":"external_event","content":"hello"}`)); err != nil {
				t.Fatal(err)
			}
			release()
			if !spawner.released {
				t.Fatal("hold was not released")
			}
		} else if !errors.Is(err, context.DeadlineExceeded) || !spawner.released {
			t.Fatalf("cancel did not release hold: %v", err)
		}
	}
}

func TestAttentionLookupRejectsUnconfirmedSyncAndRollback(t *testing.T) {
	ctx := context.Background()
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	p := attentionTestProvenance()
	body := json.RawMessage(`{"type":"external_event","content":"source"}`)
	if _, err := store.Append(ctx, p, "first", body); err != nil {
		t.Fatal(err)
	}
	failing := injectFailingFile(t, store, p.PersonalityAgentID)
	failing.failSyncOn = 1
	failing.failTruncateOn = 1
	if _, err := store.Append(ctx, p, "uncertain", body); err == nil {
		t.Fatal("fault injection did not fail")
	}
	if _, found, err := store.Lookup(ctx, p, "uncertain", body); found || err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("uncertain admission acknowledged: %v %v", found, err)
	}
	if _, err := store.Append(ctx, p, "uncertain", body); err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("keyed replay bypassed poisoned state: %v", err)
	}
}

func TestAttentionSecretaryVolatileReachesDebugQueue(t *testing.T) {
	gateway := openRuntimeGateway(t)
	claims := currentRuntimeClaims(t, gateway, attentionTestProvenance().PersonalityAgentID)
	queue, cancel := gateway.SubscribeBrowserVolatile(claims.PersonalityAgentID)
	defer cancel()
	envelope := Envelope{PersonalityAgentID: claims.PersonalityAgentID, Audience: AudienceSecretary, Event: json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"experienced"}}`)}
	if err := gateway.Receive(context.Background(), claims, envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case batch := <-queue:
		if len(batch.events) != 1 || batch.events[0].Audience != AudienceSecretary {
			t.Fatal("context lost")
		}
	default:
		t.Fatal("experienced delta hidden")
	}
}

func attentionSourceMessage(t *testing.T) json.RawMessage {
	t.Helper()
	p := attentionTestProvenance()
	raw, err := json.Marshal(map[string]any{"type": "message_end", "message_id": "00000000-0000-4000-8000-000000000001", "message": map[string]any{
		"role": "user", "content": []any{map[string]any{"type": "text", "text": "PRIVATE_SOURCE_TEXT"}}, "timestamp": "2026-09-08T12:00:01Z", "incoming_source": p,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestAttentionIncomingSourceRequiresSecretAudienceAndExactTarget(t *testing.T) {
	seq := uint64(1)
	e := Envelope{Seq: &seq, PersonalityAgentID: attentionTestProvenance().PersonalityAgentID, Audience: AudienceSecretary, Event: attentionSourceMessage(t)}
	if err := validateEnvelope(e); err != nil {
		t.Fatal(err)
	}
	e.Audience = AudienceDirectChat
	if validateEnvelope(e) == nil {
		t.Fatal("source-bearing input accepted for direct chat")
	}
	e.Audience = AudienceSecretary
	e.PersonalityAgentID = "018f47a2-9b3c-7def-9abc-0123456789ac"
	if validateEnvelope(e) == nil {
		t.Fatal("cross-person source accepted")
	}
	var event map[string]any
	if err := json.Unmarshal(e.Event, &event); err != nil {
		t.Fatal(err)
	}
	event["message"].(map[string]any)["incoming_source"] = testDirectChatProvenance(attentionTestProvenance().PersonalityAgentID)
	e.PersonalityAgentID = attentionTestProvenance().PersonalityAgentID
	e.Event, _ = json.Marshal(event)
	if validateEnvelope(e) == nil {
		t.Fatal("direct chat provenance accepted as external source")
	}
}

func TestAttentionWebSocketShowsSourceAndActionsWithArtifactRedactionLiveAndReplay(t *testing.T) {
	gateway := openRuntimeGateway(t)
	pa := attentionTestProvenance().PersonalityAgentID
	receipt := "ready"
	if err := gateway.PublishRuntimeState(pa, 7, &receipt); err != nil {
		t.Fatal(err)
	}
	sessions, err := NewHMACUserSessionVerifier(testSecret, "", newTestBrowserSessionRevocationStore())
	if err != nil {
		t.Fatal(err)
	}
	server := newAuthorizedBrowserServer(sessions, gateway, gateway)
	server.AllowedOrigins = []string{"https://web.example"}
	httpServer := httptest.NewServer(server)
	defer httpServer.Close()
	cookie := signBrowserSession(t, testSecret, userSessionWireClaims{TenantID: "tenant-1", UserID: "user-1", PersonalityAgentID: pa, Exp: time.Now().Add(time.Hour).Unix(), Aud: defaultBrowserAudience})
	connect := func(last uint64) *websocket.Conn {
		c := dialBrowserWS(t, httpServer, cookie, pa)
		if err := c.WriteJSON(browserHello{Type: "hello", LastEventSeq: last}); err != nil {
			t.Fatal(err)
		}
		return c
	}
	conn := connect(0)
	defer conn.Close()
	assertDirectChatStatus(t, conn, "ready")
	claims := TokenClaims{TenantID: "tenant-1", PersonalityAgentID: pa, Generation: 7}
	hidden := []json.RawMessage{attentionSourceMessage(t), json.RawMessage(`{"type":"tool_execution_start","tool_call_id":"call-private","tool_name":"read_file","args":{"path":"PRIVATE_TOOL_ARGS"}}`), json.RawMessage(`{"type":"tool_execution_end","tool_call_id":"call-private","result":{"text":"PRIVATE_TOOL_RESULT","handle":"artifact://018f47a2-9b3c-7def-8abc-0123456789ab/tool-output/private"},"is_error":false}`)}
	for i, event := range hidden {
		seq := uint64(i + 1)
		if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: pa, Audience: AudienceSecretary, Event: event}); err != nil {
			t.Fatal(err)
		}
	}
	seq := uint64(4)
	if err := gateway.Receive(context.Background(), claims, Envelope{Seq: &seq, PersonalityAgentID: pa, Audience: AudienceDirectChat, Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatal(err)
	}
	readVisible := func(c *websocket.Conn, last uint64) {
		t.Helper()
		for expected := last + 1; expected <= 4; expected++ {
			c.SetReadDeadline(time.Now().Add(time.Second))
			_, raw, err := c.ReadMessage()
			if err != nil {
				t.Fatal(err)
			}
			var frame browserEventFrame
			if err := json.Unmarshal(raw, &frame); err != nil {
				t.Fatalf("debug frame: %v", err)
			}
			if frame.Envelope.Seq == nil || *frame.Envelope.Seq != expected {
				t.Fatalf("sequence: %s", raw)
			}
			if expected <= 3 && frame.Envelope.Audience != AudienceSecretary {
				t.Fatal("execution context missing")
			}
			if expected == 1 && (!bytes.Contains(raw, []byte("incoming_source")) || !bytes.Contains(raw, []byte("PRIVATE_SOURCE_TEXT"))) {
				t.Fatal("received experience hidden")
			}
			if expected == 2 && !bytes.Contains(raw, []byte("PRIVATE_TOOL_ARGS")) {
				t.Fatal("action hidden")
			}
			if expected == 3 {
				if !bytes.Contains(raw, []byte("PRIVATE_TOOL_RESULT")) || !bytes.Contains(raw, []byte("artifact://tool-output/private")) {
					t.Fatal("result missing")
				}
				if bytes.Contains(raw, []byte("artifact://"+pa)) {
					t.Fatal("canonical artifact owner leaked")
				}
			}
		}
	}
	readVisible(conn, 0)
	conn.Close()
	reload := connect(0)
	defer reload.Close()
	readVisible(reload, 0)
	assertDirectChatStatus(t, reload, "ready")
	resumed := connect(3)
	defer resumed.Close()
	readVisible(resumed, 3)
	assertDirectChatStatus(t, resumed, "ready")
}

type deadlineAttentionSpawner struct {
	t     *testing.T
	block bool
}

func (s deadlineAttentionSpawner) Touch(string) {}
func (s deadlineAttentionSpawner) EnsureRunning(ctx context.Context, _ string) error {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 30*time.Second {
		s.t.Fatal("runtime startup has no bounded operation deadline")
	}
	if s.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return errors.New("runtime unavailable")
}
func TestAttentionPreparationBoundsStartupAndHonorsCancellation(t *testing.T) {
	gateway := openRuntimeGateway(t)
	pa := attentionTestProvenance().PersonalityAgentID
	if _, err := gateway.PrepareAttention(context.Background(), deadlineAttentionSpawner{t: t}, pa); err == nil {
		t.Fatal("unavailable runtime accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gateway.PrepareAttention(ctx, deadlineAttentionSpawner{t: t, block: true}, pa); !errors.Is(err, context.Canceled) {
		t.Fatalf("startup cancellation: %v", err)
	}
}
