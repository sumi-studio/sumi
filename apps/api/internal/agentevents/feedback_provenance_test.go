package agentevents

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func feedbackTestProvenance() IncomingProvenance {
	return IncomingProvenance{Version: 2, TenantID: "test", PersonalityAgentID: "018f47a2-9b3c-7def-8abc-0123456789ab", Actor: ProvenanceActor{Kind: "human", PrincipalID: "author", DisplayName: "開発者"}, Source: ProvenanceSource{Surface: "feedback", Kind: "feedback_reply", EventID: "018f47a2-9b3c-7def-8abc-0123456789ac", ThreadID: "018f47a2-9b3c-7def-8abc-0123456789ad", Title: "通知について", Revision: 2, OccurredAt: "2026-09-11T10:00:00Z"}}
}
func TestFeedbackProvenanceRoundTripAndAuthority(t *testing.T) {
	p := feedbackTestProvenance()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var got IncomingProvenance
	if err = json.Unmarshal(raw, &got); err != nil || !p.Equal(got) {
		t.Fatalf("roundtrip: %v", err)
	}
	if err = validateIncomingCommand(p, json.RawMessage(`{"type":"external_event","content":"修正しました"}`)); err != nil {
		t.Fatal(err)
	}
	if err = validateIncomingCommand(p, json.RawMessage(`{"type":"approval_decision","request_id":"x","decision":{"type":"approve"}}`)); err == nil {
		t.Fatal("feedback acquired approval authority")
	}
	for name, mutate := range map[string]func(*IncomingProvenance){"revision": func(p *IncomingProvenance) { p.Source.Revision = 0 }, "thread": func(p *IncomingProvenance) { p.Source.ThreadID = "invalid" }, "blank title": func(p *IncomingProvenance) { p.Source.Title = " " }, "long title": func(p *IncomingProvenance) { p.Source.Title = strings.Repeat("あ", 161) }, "kind": func(p *IncomingProvenance) { p.Source.Kind = "messaging_message" }, "foreign source": func(p *IncomingProvenance) { p.Source.Place = &ProvenancePlace{} }, "time": func(p *IncomingProvenance) { p.Source.OccurredAt = "yesterday" }} {
		t.Run(name, func(t *testing.T) {
			bad := p
			mutate(&bad)
			if bad.Validate() == nil {
				t.Fatal("accepted invalid source")
			}
		})
	}
	var generic map[string]any
	_ = json.Unmarshal(raw, &generic)
	source := generic["source"].(map[string]any)
	for _, key := range []string{"workspace_id", "place", "operation_id"} {
		source[key] = nil
		b, _ := json.Marshal(generic)
		if json.Unmarshal(b, &got) == nil {
			t.Fatalf("accepted extra %s", key)
		}
		delete(source, key)
	}
}

func TestFeedbackAdmissionSurvivesRestartWithOriginalSource(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	p := feedbackTestProvenance()
	command := json.RawMessage(`{"type":"external_event","content":"元の返信"}`)
	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Append(ctx, p, "feedback:stable-key", command)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	again, found, err := store.Lookup(ctx, p, "feedback:stable-key", command)
	if err != nil || !found || first.CommandID != again.CommandID || first.Seq != again.Seq || !again.Provenance.Equal(p) {
		t.Fatalf("feedback admission changed on restart: %+v %v %v", again, found, err)
	}
}
