package agentstate

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// historyContract is contracts/conversation-history-fixtures.json: the
// conversation_history request contract shared with the Core FakeState
// double (apps/core/test/history-tool-contract.test.ts runs the same cases).
type historyContract struct {
	Normalization struct {
		Error string `json:"error"`
	} `json:"openai_responses_strict_normalization"`
	Cases []struct {
		Name    string          `json:"name"`
		Request json.RawMessage `json:"request"`
		OK      bool            `json:"ok"`
		Error   string          `json:"error"`
	} `json:"cases"`
}

func loadHistoryContract(t *testing.T) historyContract {
	t.Helper()
	raw, err := os.ReadFile("../../../../contracts/conversation-history-fixtures.json")
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	var c historyContract
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("parse contract: %v", err)
	}
	if len(c.Cases) == 0 {
		t.Fatal("contract has no cases")
	}
	return c
}

// decodeRequest decodes a request the way the tool call reaches the store:
// JSON, so numbers are float64 and null is a present nil.
func decodeRequest(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	return m
}

// Every contract case is accepted, or rejected with its exact message, by
// the store's argument parser — the same verdicts the FakeState gives.
func TestHistoryContractArguments(t *testing.T) {
	c := loadHistoryContract(t)
	for _, tc := range c.Cases {
		_, err := parseHistoryArgs(decodeRequest(t, tc.Request))
		switch {
		case tc.OK && err != nil:
			t.Errorf("%s: want accepted, got %v", tc.Name, err)
		case !tc.OK && err == nil:
			t.Errorf("%s: want %q, got accepted", tc.Name, tc.Error)
		case !tc.OK && (!errors.Is(err, ErrBadRequest) || !strings.HasSuffix(err.Error(), ": "+tc.Error)):
			t.Errorf("%s: want bad request %q, got %v", tc.Name, tc.Error, err)
		}
	}
}

// The hosted Q2 failure end to end in the store: the four requests the real
// model sent under Responses' strict normalization are each a bad request,
// while the read the canonical schema lets a model express returns the
// chunk's original records.
func TestHistoryContractObservedAndCorrectedReads(t *testing.T) {
	c := loadHistoryContract(t)
	s, _ := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)
	gen := acquireWriter(t, s, pa, time.Minute)
	chunk := seedSealed(t, s, pa, gen, 2)

	run := func(request map[string]any) (map[string]any, error) {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		return s.conversationHistory(ctx, tx, pa, request)
	}

	observed := 0
	for _, tc := range c.Cases {
		if !strings.HasPrefix(tc.Name, "observed ") {
			continue
		}
		observed++
		if _, err := run(decodeRequest(t, tc.Request)); !errors.Is(err, ErrBadRequest) ||
			!strings.Contains(err.Error(), c.Normalization.Error) {
			t.Fatalf("%s: want %q, got %v", tc.Name, c.Normalization.Error, err)
		}
	}
	if observed != 4 {
		t.Fatalf("contract carries %d observed calls, want the 4 from the hosted run", observed)
	}

	res, err := run(decodeRequest(t, json.RawMessage(`{"operation":"read","chunk_seq":1}`)))
	if err != nil {
		t.Fatalf("corrected read: %v", err)
	}
	msgs := historyMessages(asModelSees(t, res))
	if len(msgs) == 0 {
		t.Fatalf("corrected read returned no records: %+v", res)
	}
	for _, m := range msgs {
		seq := int64(m["source"].(map[string]any)["seq"].(float64))
		if seq < chunk.FirstSeq || seq > chunk.LastSeq {
			t.Fatalf("record seq %d outside chunk %d..%d", seq, chunk.FirstSeq, chunk.LastSeq)
		}
	}
}

// asModelSees passes a result through JSON as the model receives it.
func asModelSees(t *testing.T, v map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}
