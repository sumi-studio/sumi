package agentevents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestBrowserMergedDeltaBoundIncludesJSONEscaping(t *testing.T) {
	const id = "018f47a2-9b3c-7def-8abc-0123456789ab"
	raw := json.RawMessage(`{"type":"message_update","message_id":"a","event":{"type":"text_delta","content_index":0,"delta":"` + strings.Repeat("<", 2000) + `"}}`)
	event := Envelope{Audience: AudienceDirectChat, PersonalityAgentID: id, Event: raw}
	var batch browserVolatileBatch
	if !batch.append(event) || !batch.append(event) {
		t.Fatal("separate bounded frames should fit")
	}
	if len(batch.events) != 2 {
		t.Fatal("escaped merge exceeds per-frame byte cap")
	}
}

func TestBrowserVolatileBurstKeepsEveryDelta(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const id = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := currentRuntimeClaims(t, gateway, id)
	stream, unsubscribe := gateway.SubscribeBrowserVolatile(id)
	defer unsubscribe()
	const count = 1000
	for i := 0; i < count; i++ {
		if err := gateway.Receive(context.Background(), claims, Envelope{Audience: AudienceDirectChat,
			PersonalityAgentID: id,
			Event:              json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"あ"}}`),
		}); err != nil {
			t.Fatal(err)
		}
	}
	var got strings.Builder
	for {
		select {
		case batch, ok := <-stream:
			if !ok {
				t.Fatal("ordinary token burst disconnected browser")
			}
			for _, envelope := range batch.events {
				var update struct{ Event struct{ Delta string } }
				if err := json.Unmarshal(envelope.Event, &update); err != nil {
					t.Fatal(err)
				}
				got.WriteString(update.Event.Delta)
			}
		default:
			if got.String() != strings.Repeat("あ", count) {
				t.Fatalf("received %d bytes, want %d", got.Len(), len(strings.Repeat("あ", count)))
			}
			return
		}
	}
}

func TestBrowserVolatileBatchPreservesInterleavedBoundaries(t *testing.T) {
	const id = "018f47a2-9b3c-7def-8abc-0123456789ab"
	makeDelta := func(message, kind string, index int, delta string) Envelope {
		raw := fmt.Sprintf(`{"type":"message_update","message_id":%q,"event":{"type":%q,"content_index":%d,"delta":%q}}`, message, kind, index, delta)
		return Envelope{Audience: AudienceDirectChat, PersonalityAgentID: id, Event: json.RawMessage(raw)}
	}
	first := makeDelta("a", "text_delta", 0, "a")
	for _, boundary := range []Envelope{
		makeDelta("b", "text_delta", 0, "b"),
		makeDelta("a", "thinking_delta", 0, "b"),
		makeDelta("a", "text_delta", 1, "b"),
		{Audience: AudienceDirectChat, PersonalityAgentID: id, Event: json.RawMessage(`{"type":"message_update","message_id":"a","event":{"type":"text_end","content_index":0,"content":"a"}}`)},
		{Audience: AudienceDirectChat, PersonalityAgentID: id, Event: json.RawMessage(`{"type":"error","message":"retained"}`)},
	} {
		batch := browserVolatileBatch{}
		for _, event := range []Envelope{first, boundary, first} {
			if !batch.append(event) {
				t.Fatal("unexpected overflow")
			}
		}
		if len(batch.events) != 3 || string(batch.events[1].Event) != string(boundary.Event) {
			t.Fatal("merged across interleaved boundary")
		}
	}
	for _, kind := range []string{"text_delta", "thinking_delta", "tool_call_delta", "reasoning_summary_delta"} {
		batch := browserVolatileBatch{}
		if !batch.append(makeDelta("a", kind, 0, "あ")) || !batch.append(makeDelta("a", kind, 0, "い")) {
			t.Fatal("unexpected overflow")
		}
		var merged browserMessageDelta
		if len(batch.events) != 1 {
			t.Fatal("compatible delta did not combine")
		}
		if err := json.Unmarshal(batch.events[0].Event, &merged); err != nil {
			t.Fatal(err)
		}
		if merged.Event.Delta != "あい" {
			t.Fatal("delta contents changed")
		}
	}
}

func TestBrowserVolatileQueueBoundsIndependentEventsAndBytes(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const id = "018f47a2-9b3c-7def-8abc-0123456789ab"
	stream, unsubscribe := gateway.SubscribeBrowserVolatile(id)
	defer unsubscribe()
	event := Envelope{Audience: AudienceDirectChat, PersonalityAgentID: id, Event: json.RawMessage(`{"type":"error","message":"retained"}`)}
	gateway.mu.Lock()
	for i := 0; i <= maxBrowserVolatileEvents; i++ {
		gateway.publishVolatileLocked(id, event)
	}
	gateway.mu.Unlock()
	if _, ok := <-stream; ok {
		t.Fatal("unbounded independent event queue")
	}
	batch := browserVolatileBatch{}
	large := event
	large.Event = make([]byte, maxBrowserVolatileBytes)
	if !batch.append(large) {
		t.Fatal("one frame up to server read limit should fit")
	}
	if batch.append(event) {
		t.Fatal("byte budget not enforced")
	}
}

func TestBrowserBurstPumpPreservesDurableCompletionAndPrefix(t *testing.T) {
	gateway := openRuntimeGateway(t)
	const id = "018f47a2-9b3c-7def-8abc-0123456789ab"
	claims := currentRuntimeClaims(t, gateway, id)
	stream, unsubscribe := gateway.SubscribeBrowserVolatile(id)
	defer unsubscribe()
	seq := uint64(1)
	if err := gateway.Receive(context.Background(), claims, Envelope{Audience: AudienceDirectChat, Seq: &seq, PersonalityAgentID: id, Event: json.RawMessage(`{"type":"agent_start"}`)}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if err := gateway.Receive(context.Background(), claims, Envelope{Audience: AudienceDirectChat, PersonalityAgentID: id, Event: json.RawMessage(`{"type":"message_update","message_id":"00000000-0000-4000-8000-000000000001","event":{"type":"text_delta","content_index":0,"delta":"x"}}`)}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var sequences []uint64
	var received strings.Builder
	write := func(raw any) error {
		frame, ok := raw.(browserEventFrame)
		if !ok {
			return nil
		}
		if frame.Envelope.Seq != nil {
			sequences = append(sequences, *frame.Envelope.Seq)
			if *frame.Envelope.Seq == 2 {
				cancel()
			}
		} else {
			if len(sequences) == 0 || sequences[0] != 1 {
				t.Fatal("volatile arrived before durable prefix")
			}
			var update browserMessageDelta
			if err := json.Unmarshal(frame.Envelope.Event, &update); err != nil {
				return err
			}
			received.WriteString(update.Event.Delta)
			// Completion arrives while the consumer drains the burst.
			if received.Len() == 1000 {
				next := uint64(2)
				return gateway.Receive(ctx, claims, Envelope{Audience: AudienceDirectChat, Seq: &next, PersonalityAgentID: id, Event: json.RawMessage(`{"type":"agent_end"}`)})
			}
		}
		return nil
	}
	server := BrowserServer{Events: gateway}
	if err := server.browserEventPump(ctx, id, 0, directChatReadiness{}, stream, nil, write); !errors.Is(err, context.Canceled) {
		t.Fatalf("pump: %v", err)
	}
	if received.String() != strings.Repeat("x", 1000) || len(sequences) != 2 || sequences[1] != 2 {
		t.Fatalf("incomplete stream: text=%d seqs=%v", received.Len(), sequences)
	}
}
