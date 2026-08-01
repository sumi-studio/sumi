package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func testInboundProvenance(personalityAgentID string) InboundProvenance {
	return InboundProvenance{
		Version:            1,
		TenantID:           "tenant-1",
		PersonalityAgentID: personalityAgentID,
		Actor: ProvenanceActor{
			Kind:    "human",
			HumanID: "018f47a2-9b3c-7def-8abc-00000000ab01",
		},
		Source:    ProvenanceSource{Surface: "direct_chat"},
		Authority: AdmissionAuthority{Basis: "employer"},
	}
}

func testCommandEnvelope(seq uint64, commandID string, command json.RawMessage, personalityAgentID string) CommandEnvelope {
	return CommandEnvelope{
		Seq:                seq,
		CommandID:          commandID,
		PersonalityAgentID: personalityAgentID,
		Provenance:         testInboundProvenance(personalityAgentID),
		Command:            command,
	}
}

func testLogRecord(seq uint64, commandID string, command json.RawMessage, personalityAgentID string) LogRecord {
	return LogRecord{CommandEnvelope: testCommandEnvelope(seq, commandID, command, personalityAgentID)}
}

func TestValidatePersonalityAgentIDCanonicalUUIDv7(t *testing.T) {
	valid := "018f47a2-9b3c-7def-8abc-0123456789ab"
	if err := ValidatePersonalityAgentID(valid); err != nil {
		t.Fatalf("canonical UUIDv7 rejected: %v", err)
	}
	for name, value := range map[string]string{
		"uuid_v1":       "018f47a2-9b3c-1def-8abc-0123456789ab",
		"uuid_v4":       "018f47a2-9b3c-4def-8abc-0123456789ab",
		"uuid_v6":       "018f47a2-9b3c-6def-8abc-0123456789ab",
		"wrong_variant": "018f47a2-9b3c-7def-7abc-0123456789ab",
		"uppercase":     "018F47A2-9B3C-7DEF-8ABC-0123456789AB",
		"braces":        "{018f47a2-9b3c-7def-8abc-0123456789ab}",
		"compact":       "018f47a29b3c7def8abc0123456789ab",
		"leading_ws":    " 018f47a2-9b3c-7def-8abc-0123456789ab",
		"trailing_ws":   "018f47a2-9b3c-7def-8abc-0123456789ab ",
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidatePersonalityAgentID(value); err == nil {
				t.Fatalf("accepted noncanonical personality agent ID %q", value)
			}
		})
	}
}

func TestCommandEnvelopeRejectsMalformedProvenanceAndTargetMismatch(t *testing.T) {
	base := `{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","provenance":%s,"command":{"type":"abort"}}`
	valid := `{"version":1,"tenant_id":"tenant-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","actor":{"kind":"human","human_id":"018f47a2-9b3c-7def-8abc-00000000ab01"},"source":{"surface":"direct_chat"},"authority":{"basis":"employer","decision_id":null}}`
	// 拒否ケースが「authority を書き忘れたから落ちた」で通ってしまわないよう、
	// 土台が受理されることを先に確かめる。
	var accepted CommandEnvelope
	if err := json.Unmarshal([]byte(fmt.Sprintf(base, valid)), &accepted); err != nil {
		t.Fatalf("rejected a well-formed direct chat provenance: %v", err)
	}
	for name, provenance := range map[string]string{
		"missing":           `null`,
		"unknown":           strings.TrimSuffix(valid, "}") + `,"extra":true}`,
		"missing_actor":     `{"version":1,"tenant_id":"tenant-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","source":{"surface":"direct_chat"},"authority":{"basis":"employer","decision_id":null}}`,
		"unknown_actor":     `{"version":1,"tenant_id":"tenant-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","actor":{"kind":"human","human_id":"018f47a2-9b3c-7def-8abc-00000000ab01","extra":true},"source":{"surface":"direct_chat"},"authority":{"basis":"employer","decision_id":null}}`,
		"wrong_source":      strings.Replace(valid, "direct_chat", "task", 1),
		"missing_authority": strings.Replace(valid, ","+`"authority":{"basis":"employer","decision_id":null}`, "", 1),
		// Firebase principal は credential であって identity ではない（ADR 0009 §2）。
		"credential_as_human_id": strings.Replace(valid, "018f47a2-9b3c-7def-8abc-00000000ab01", "alice@example.com", 1),
		// 明示的な null も含め、direct chat の形は閉じている（ADR 0009 §5）。
		"direct_chat_with_null_place": strings.Replace(
			valid,
			`{"surface":"direct_chat"}`,
			`{"surface":"direct_chat","place":null}`,
			1,
		),
		// direct chat に place は無い（ADR 0009 §5）。
		"direct_chat_with_place": strings.Replace(
			valid,
			`{"surface":"direct_chat"}`,
			`{"surface":"direct_chat","place":{"kind":"channel","channel_id":"ch-1"}}`,
			1,
		),
		"target_mismatch":  strings.Replace(valid, "018f47a2-9b3c-7def-8abc-0123456789ab", "018f47a2-9b3c-7def-9abc-0123456789ac", 1),
		"oversized_tenant": strings.Replace(valid, "tenant-1", strings.Repeat("a", 257), 1),
	} {
		t.Run(name, func(t *testing.T) {
			var envelope CommandEnvelope
			if err := json.Unmarshal([]byte(fmt.Sprintf(base, provenance)), &envelope); err == nil {
				t.Fatalf("accepted malformed provenance: %s", provenance)
			}
		})
	}
}

func TestCommandStoreIdempotencyBindsAuthenticatedEnvelopeAcrossTargets(t *testing.T) {
	dir := t.TempDir()
	firstStore, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer firstStore.Close()
	secondStore, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()

	command := json.RawMessage(`{"type":"user_message","text":"hello","attachments":[]}`)
	first := testInboundProvenance("018f47a2-9b3c-7def-8abc-0123456789ab")
	second := testInboundProvenance("018f47a2-9b3c-7def-9abc-0123456789ac")
	type result struct{ err error }
	results := make(chan result, 2)
	var start sync.WaitGroup
	start.Add(1)
	for _, input := range []struct {
		store      *CommandStore
		provenance InboundProvenance
	}{{firstStore, first}, {secondStore, second}} {
		go func() {
			start.Wait()
			_, err := input.store.Append(context.Background(), input.provenance, "global-key", command)
			results <- result{err: err}
		}()
	}
	start.Done()
	var accepted, conflicts int
	for range 2 {
		switch err := (<-results).err; {
		case err == nil:
			accepted++
		case errors.Is(err, errIdempotencyConflict):
			conflicts++
		default:
			t.Fatalf("unexpected append result: %v", err)
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("cross-target keyed race: accepted=%d conflicts=%d", accepted, conflicts)
	}

	winner := first
	if commands, _ := firstStore.CatchUp(context.Background(), first.PersonalityAgentID, 1); len(commands) == 0 {
		winner = second
	}
	retry, err := firstStore.Append(context.Background(), winner, "global-key", command)
	if err != nil {
		t.Fatalf("identical retry failed: %v", err)
	}
	if retry.Seq != 1 {
		t.Fatalf("identical retry allocated a new seq: %+v", retry)
	}
	for name, mutate := range map[string]func(*InboundProvenance){
		"tenant": func(p *InboundProvenance) { p.TenantID = "tenant-2" },
		"actor":  func(p *InboundProvenance) { p.Actor.HumanID = "018f47a2-9b3c-7def-8abc-00000000ab02" },
		"source": func(p *InboundProvenance) { p.Source.Surface = "task" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := winner
			mutate(&changed)
			if _, err := firstStore.Append(context.Background(), changed, "global-key", command); !errors.Is(err, errIdempotencyConflict) {
				t.Fatalf("expected provenance conflict, got %v", err)
			}
		})
	}
	if _, err := firstStore.Append(context.Background(), winner, "global-key", json.RawMessage(`{"type":"abort"}`)); !errors.Is(err, errIdempotencyConflict) {
		t.Fatalf("expected command conflict, got %v", err)
	}
}

func TestCommandStoreIdempotencySerializesCrossTargetRaceWithinOneStore(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	command := json.RawMessage(`{"type":"user_message","text":"same-store","attachments":[]}`)
	provenances := []InboundProvenance{
		testInboundProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"),
		testInboundProvenance("018f47a2-9b3c-7def-9abc-0123456789ac"),
	}
	results := make(chan error, len(provenances))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, provenance := range provenances {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.Append(context.Background(), provenance, "same-store-global-key", command)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var accepted, conflicts int
	for err := range results {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, errIdempotencyConflict):
			conflicts++
		default:
			t.Fatalf("unexpected same-store append result: %v", err)
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("same-store cross-target race: accepted=%d conflicts=%d", accepted, conflicts)
	}
}

func TestCommandStoreIdempotencyGuardHonorsContextCancellation(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.idempotencyGuard <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_, appendErr := store.Append(
		ctx,
		testInboundProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"),
		"waiting-key",
		json.RawMessage(`{"type":"abort"}`),
	)
	<-store.idempotencyGuard
	if !errors.Is(appendErr, context.DeadlineExceeded) {
		t.Fatalf("guard wait ignored context cancellation: %v", appendErr)
	}
}

func TestCommandStoreCloseWaitsForIdempotencyGuardBeforeClosingFlock(t *testing.T) {
	store, err := OpenCommandStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	store.idempotencyGuard <- struct{}{}
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- store.Close()
	}()

	deadline := time.Now().Add(time.Second)
	for {
		store.mu.Lock()
		closed := store.closed
		store.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not enter closed state")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closeResult:
		t.Fatalf("Close returned before the idempotency guard was released: %v", err)
	default:
	}

	<-store.idempotencyGuard
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the idempotency guard was released")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := store.Append(
		ctx,
		testInboundProvenance("018f47a2-9b3c-7def-8abc-0123456789ab"),
		"after-close",
		json.RawMessage(`{"type":"abort"}`),
	); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("keyed append after Close did not fail promptly as closed: %v", err)
	}
}

func TestCommandStoreRestartRejectsLegacyTargetlessRecord(t *testing.T) {
	dir := t.TempDir()
	personalityAgentID := "018f47a2-9b3c-7def-8abc-0123456789ab"
	legacy := `{"seq":1,"command_id":"00000000-0000-4000-8000-000000000001","command":{"type":"abort"}}` + "\n"
	if err := os.WriteFile(commandLogPath(dir, personalityAgentID), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	if store, err := OpenCommandStore(dir); err == nil {
		_ = store.Close()
		t.Fatal("legacy targetless command record must fail closed after restart")
	}
}

func TestRuntimeGenerationIsGlobalByPersonalityAgentID(t *testing.T) {
	gateway := openRuntimeGateway(t)
	personalityAgentID := "018f47a2-9b3c-7def-8abc-0123456789ab"
	otherPersonalityAgentID := "018f47a2-9b3c-7def-9abc-0123456789ac"
	receipt := "hydrated-1"
	if err := gateway.PublishRuntimeState(personalityAgentID, 7, &receipt); err != nil {
		t.Fatal(err)
	}
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := gateway.WaitFor(ctx, TokenClaims{
			TenantID:           tenantID,
			PersonalityAgentID: personalityAgentID,
			Generation:         7,
		}, 7)
		cancel()
		if err != nil {
			t.Fatalf("same personality in %s did not share generation: %v", tenantID, err)
		}
	}
	if ready, err := gateway.IsPersonalityAgentReady(context.Background(), otherPersonalityAgentID); err != nil || ready {
		t.Fatalf("distinct personality reused runtime state: ready=%v err=%v", ready, err)
	}
}

// メッセージング provenance は API が発行しない（正本は Workspace API、
// ADR 0011 §10）が、すべての Surface はこの durable command 経路を通って agent
// へ届くため、Go 側でも表現・検証できなければならない。
func TestMessagingProvenanceRoundTripsAndEnforcesItsShape(t *testing.T) {
	const speaker = "018f47a2-9b3c-7def-8abc-000000000a2c"
	workspaceID := "ws-1"
	correlationID := "corr-1"
	provenance := InboundProvenance{
		Version:            1,
		TenantID:           "tenant-1",
		PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab",
		Actor: ProvenanceActor{
			Kind:               "personality_agent",
			PersonalityAgentID: speaker,
		},
		Source: ProvenanceSource{
			Surface:     "messaging",
			WorkspaceID: &workspaceID,
			Place:       &ProvenancePlace{Kind: "channel", ChannelID: "ch-general"},
			Delivery: &ProvenanceDelivery{
				MessageID: "msg-1",
				Seq:       42,
				Addressees: []ProvenanceActor{
					{Kind: "human", HumanID: "018f47a2-9b3c-7def-8abc-00000000ab01"},
				},
				TriggerReason: "mention",
				Urgency:       "urgent",
				CorrelationID: &correlationID,
			},
		},
		Authority: AdmissionAuthority{Basis: "place_membership"},
	}
	if err := provenance.Validate(); err != nil {
		t.Fatalf("well-formed messaging provenance rejected: %v", err)
	}
	encoded, err := json.Marshal(provenance)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded InboundProvenance
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	reencoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if string(encoded) != string(reencoded) {
		t.Fatalf("round-trip changed the wire bytes:\n%s\n%s", encoded, reencoded)
	}

	for name, mutate := range map[string]func(*InboundProvenance){
		// messaging は必ず配送されたメッセージを伴う（ADR 0011 §1）。
		"missing_delivery": func(p *InboundProvenance) { p.Source.Delivery = nil },
		"missing_place":    func(p *InboundProvenance) { p.Source.Place = nil },
		"channel_with_dm_id": func(p *InboundProvenance) {
			p.Source.Place = &ProvenancePlace{Kind: "channel", ChannelID: "ch-1", DmID: "dm-1"}
		},
		"agent_actor_with_human_id": func(p *InboundProvenance) {
			p.Actor.HumanID = "018f47a2-9b3c-7def-8abc-00000000ab01"
		},
		"unknown_trigger_reason": func(p *InboundProvenance) { p.Source.Delivery.TriggerReason = "summary" },
		"unknown_urgency":        func(p *InboundProvenance) { p.Source.Delivery.Urgency = "critical" },
		"unknown_authority":      func(p *InboundProvenance) { p.Authority.Basis = "admin" },
		"null_addressees":        func(p *InboundProvenance) { p.Source.Delivery.Addressees = nil },
		// alias を place 全体へ展開した結果は provenance に載せない（ADR 0011 §2）。
		"expanded_broadcast": func(p *InboundProvenance) {
			addressees := make([]ProvenanceActor, 0, maxProvenanceAddressees+1)
			for i := 0; i <= maxProvenanceAddressees; i++ {
				addressees = append(addressees, ProvenanceActor{
					Kind:    "human",
					HumanID: "018f47a2-9b3c-7def-8abc-00000000ab01",
				})
			}
			p.Source.Delivery.Addressees = addressees
		},
	} {
		t.Run(name, func(t *testing.T) {
			mutated := provenance
			delivery := *provenance.Source.Delivery
			mutated.Source.Delivery = &delivery
			mutate(&mutated)
			if err := mutated.Validate(); err == nil {
				t.Fatalf("accepted malformed messaging provenance")
			}
		})
	}
}
