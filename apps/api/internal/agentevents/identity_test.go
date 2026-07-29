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

func testDirectChatProvenance(personalityAgentID string) DirectChatProvenance {
	return DirectChatProvenance{
		Version:            1,
		TenantID:           "tenant-1",
		PersonalityAgentID: personalityAgentID,
		Actor: ProvenanceActor{
			Kind:        "human",
			PrincipalID: "user-1",
		},
		Source: ProvenanceSource{Surface: "direct_chat"},
	}
}

func testCommandEnvelope(seq uint64, commandID string, command json.RawMessage, personalityAgentID string) CommandEnvelope {
	return CommandEnvelope{
		Seq:                seq,
		CommandID:          commandID,
		PersonalityAgentID: personalityAgentID,
		Provenance:         testDirectChatProvenance(personalityAgentID),
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
	valid := `{"version":1,"tenant_id":"tenant-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","actor":{"kind":"human","principal_id":"user-1"},"source":{"surface":"direct_chat"}}`
	for name, provenance := range map[string]string{
		"missing":          `null`,
		"unknown":          strings.TrimSuffix(valid, "}") + `,"extra":true}`,
		"missing_actor":    `{"version":1,"tenant_id":"tenant-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","source":{"surface":"direct_chat"}}`,
		"unknown_actor":    `{"version":1,"tenant_id":"tenant-1","personality_agent_id":"018f47a2-9b3c-7def-8abc-0123456789ab","actor":{"kind":"human","principal_id":"user-1","extra":true},"source":{"surface":"direct_chat"}}`,
		"wrong_source":     strings.Replace(valid, "direct_chat", "task", 1),
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
	first := testDirectChatProvenance("018f47a2-9b3c-7def-8abc-0123456789ab")
	second := testDirectChatProvenance("018f47a2-9b3c-7def-9abc-0123456789ac")
	type result struct{ err error }
	results := make(chan result, 2)
	var start sync.WaitGroup
	start.Add(1)
	for _, input := range []struct {
		store      *CommandStore
		provenance DirectChatProvenance
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
	for name, mutate := range map[string]func(*DirectChatProvenance){
		"tenant": func(p *DirectChatProvenance) { p.TenantID = "tenant-2" },
		"actor":  func(p *DirectChatProvenance) { p.Actor.PrincipalID = "user-2" },
		"source": func(p *DirectChatProvenance) { p.Source.Surface = "task" },
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
