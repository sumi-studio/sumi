package agentevents

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

type fakeCommandAppender struct {
	mu      sync.Mutex
	calls   []appendCall
	nextSeq uint64
}

type appendCall struct {
	ConversationID string
	IdempotencyKey string
	Command        json.RawMessage
}

type fakeTokenVerifier struct {
	mu             sync.Mutex
	conversationID string
	reject         bool
	err            error
}

func (f *fakeTokenVerifier) Verify(ctx context.Context, token string) (TokenClaims, error) {
	f.mu.Lock()
	reject := f.reject
	err := f.err
	conversationID := f.conversationID
	f.mu.Unlock()

	if reject {
		return TokenClaims{}, fmt.Errorf("rejected")
	}
	if err != nil {
		return TokenClaims{}, err
	}
	conv := conversationID
	if conv == "" {
		conv = "conversation-1"
	}
	return TokenClaims{
		TenantID:       "tenant-1",
		AgentID:        "agent-1",
		ConversationID: conv,
		Generation:     7,
	}, nil
}

func (f *fakeTokenVerifier) setReject(reject bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reject = reject
}

type errorReadCloser struct{}

func (errorReadCloser) Read([]byte) (int, error) {
	return 0, errors.New("body read failed")
}

func (errorReadCloser) Close() error {
	return nil
}

func (f *fakeCommandAppender) Append(ctx context.Context, conversationID string, idempotencyKey string, command json.RawMessage) (CommandEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSeq++
	f.calls = append(f.calls, appendCall{ConversationID: conversationID, IdempotencyKey: idempotencyKey, Command: command})
	return CommandEnvelope{
		Seq:       f.nextSeq,
		CommandID: fmt.Sprintf("00000000-0000-4000-8000-%012d", f.nextSeq),
		Command:   command,
	}, nil
}

func (f *fakeCommandAppender) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newTestIngress(t *testing.T) (*UserCommandIngress, *fakeCommandAppender) {
	t.Helper()
	appender := &fakeCommandAppender{}
	verifier := &fakeTokenVerifier{conversationID: "conv-1"}
	ingress, err := NewUserCommandIngress(appender, verifier)
	if err != nil {
		t.Fatalf("new ingress: %v", err)
	}
	return ingress, appender
}

func postAuthorized(t *testing.T, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func newCommandMux(ingress *UserCommandIngress) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST /conversations/{conversation_id}/commands", ingress)
	return mux
}

func TestUserCommandIngress_ValidRequestAllocatesSeq(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp := postAuthorized(t, server.URL+"/conversations/conv-1/commands", body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	if appender.callCount() != 1 {
		t.Fatalf("expected 1 append call, got %d", appender.callCount())
	}

	var env CommandEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if env.Seq != 1 {
		t.Fatalf("expected seq 1, got %d", env.Seq)
	}
	if env.CommandID == "" {
		t.Fatal("expected non-empty command_id")
	}
}

func TestUserCommandIngress_OversizedRejectedWithoutAllocatingSeq(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	// Build a command whose raw body is just over 1 MiB.
	// The text alone is at the limit; the JSON wrapper pushes the total over.
	padding := strings.Repeat("x", MaxUserCommandBytes)
	body := []byte(fmt.Sprintf(`{"type":"user_message","text":"%s","attachments":[]}`, padding))
	if len(body) <= MaxUserCommandBytes {
		t.Fatalf("test fixture not oversized: %d bytes", len(body))
	}

	resp := postAuthorized(t, server.URL+"/conversations/conv-1/commands", body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls for oversized, got %d", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectOversized)

	// A subsequent valid request must receive the first contiguous sequence.
	valid := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp2 := postAuthorized(t, server.URL+"/conversations/conv-1/commands", valid)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 after reject, got %d", resp2.StatusCode)
	}
	var env CommandEnvelope
	if err := json.NewDecoder(resp2.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Seq != 1 {
		t.Fatalf("expected first valid seq to be 1 after rejection, got %d", env.Seq)
	}
	if appender.callCount() != 1 {
		t.Fatalf("expected 1 total append call, got %d", appender.callCount())
	}
}

func TestUserCommandIngress_NonEmptyAttachmentsRejected(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[{"name":"x"}]}`)
	resp := postAuthorized(t, server.URL+"/conversations/conv-1/commands", body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectAttachmentsNotEmpty)

	valid := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp2 := postAuthorized(t, server.URL+"/conversations/conv-1/commands", valid)
	defer resp2.Body.Close()
	var env CommandEnvelope
	if err := json.NewDecoder(resp2.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Seq != 1 {
		t.Fatalf("expected first valid seq 1, got %d", env.Seq)
	}
}

func TestUserCommandIngress_MalformedAndUnknownRejected(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	cases := []struct {
		name   string
		body   string
		reason RejectReason
	}{
		{
			name:   "invalid JSON",
			body:   `{"type":"user_message","text":"hi","attachments":[]`,
			reason: RejectSchemaViolation,
		},
		{
			name:   "missing text",
			body:   `{"type":"user_message","attachments":[]}`,
			reason: RejectSchemaViolation,
		},
		{
			name:   "text not string",
			body:   `{"type":"user_message","text":123,"attachments":[]}`,
			reason: RejectSchemaViolation,
		},
		{
			name:   "unknown command type",
			body:   `{"type":"abort","text":"","attachments":[]}`,
			reason: RejectUnknownCommand,
		},
		{
			name:   "extra field",
			body:   `{"type":"user_message","text":"hi","attachments":[],"extra":1}`,
			reason: RejectSchemaViolation,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := postAuthorized(t, server.URL+"/conversations/conv-1/commands", []byte(tc.body))
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", resp.StatusCode)
			}
			if appender.callCount() != 0 {
				t.Fatalf("expected 0 append calls, got %d", appender.callCount())
			}
			assertRejectReason(t, resp.Body, tc.reason)
		})
	}
}

func TestUserCommandIngress_InvalidUTF8RejectedWithoutAllocatingSeq(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := append(
		[]byte(`{"type":"user_message","text":"`),
		0xff,
	)
	body = append(body, []byte(`","attachments":[]}`)...)
	resp := postAuthorized(t, server.URL+"/conversations/conv-1/commands", body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectSchemaViolation)
}

func TestUserCommandIngress_BodyReadFailureIsNotMisclassifiedAsOversized(t *testing.T) {
	ingress, appender := newTestIngress(t)
	req := httptest.NewRequest(
		http.MethodPost,
		"/conversations/conv-1/commands",
		nil,
	)
	req.SetPathValue("conversation_id", "conv-1")
	req.Header.Set("Authorization", "Bearer test-token")
	req.Body = errorReadCloser{}
	recorder := httptest.NewRecorder()

	ingress.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
	assertRejectReason(t, recorder.Body, RejectSchemaViolation)
}

func TestUserCommandIngress_MultipleValidRequestsAreContiguous(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	for i := 1; i <= 3; i++ {
		body := []byte(fmt.Sprintf(`{"type":"user_message","text":"msg %d","attachments":[]}`, i))
		resp := postAuthorized(t, server.URL+"/conversations/conv-1/commands", body)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("expected 201 for request %d, got %d", i, resp.StatusCode)
		}
		var env CommandEnvelope
		if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}
		resp.Body.Close()
		if env.Seq != uint64(i) {
			t.Fatalf("expected seq %d, got %d", i, env.Seq)
		}
	}
	if appender.callCount() != 3 {
		t.Fatalf("expected 3 append calls, got %d", appender.callCount())
	}
}

func TestUserCommandIngress_RequiresAuthorization(t *testing.T) {
	appender := &fakeCommandAppender{}
	ingress, err := NewUserCommandIngress(appender, nil)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /conversations/{conversation_id}/commands", ingress)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	resp := postAuthorized(t, server.URL+"/conversations/conv-1/commands", body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unconfigured verifier, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
}

func TestUserCommandIngress_RejectsInvalidAndWrongConversationToken(t *testing.T) {
	appender := &fakeCommandAppender{}
	verifier := &fakeTokenVerifier{conversationID: "other-conversation"}
	ingress, err := NewUserCommandIngress(appender, verifier)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /conversations/{conversation_id}/commands", ingress)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/conversations/conv-1/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for wrong conversation, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}

	verifier.setReject(true)
	req2, err := http.NewRequest(http.MethodPost, server.URL+"/conversations/conv-1/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer test-token")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 for rejected token, got %d", resp2.StatusCode)
	}
}

func TestUserCommandIngress_NilAppenderFailsClosed(t *testing.T) {
	if _, err := NewUserCommandIngress(nil, nil); err == nil {
		t.Fatal("expected nil appender to fail closed")
	}
}

func TestUserCommandIngress_TrailingDataRejectedWithoutAllocatingSeq(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]} trailing`)
	resp := postAuthorized(t, server.URL+"/conversations/conv-1/commands", body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectSchemaViolation)
}

func TestUserCommandIngress_OversizedIdempotencyKeyRejected(t *testing.T) {
	ingress, appender := newTestIngress(t)
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"hi","attachments":[]}`)
	req, err := http.NewRequest(http.MethodPost, server.URL+"/conversations/conv-1/commands", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Idempotency-Key", strings.Repeat("x", MaxIdempotencyKeyBytes+1))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	if appender.callCount() != 0 {
		t.Fatalf("expected 0 append calls, got %d", appender.callCount())
	}
	assertRejectReason(t, resp.Body, RejectOversized)
}

func TestUserCommandIngress_ExhaustedSeqDoesNotAllocateOrPersist(t *testing.T) {
	dir := t.TempDir()
	conv := "conv-ingress-max"

	// Seed the log with the maximum JSON-safe seq; next append should exhaust.
	seed := LogRecord{
		CommandEnvelope: CommandEnvelope{
			Seq:       maxJSONSafeInteger,
			CommandID: "00000000-0000-4000-8000-000000000000",
			Command:   json.RawMessage(`{"type":"user_message","text":"seed","attachments":[]}`),
		},
	}
	seedLine, _ := json.Marshal(seed)
	path := commandLogPath(dir, conv)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(seedLine, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := OpenCommandStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	verifier := &fakeTokenVerifier{conversationID: conv}
	ingress, err := NewUserCommandIngress(store, verifier)
	if err != nil {
		t.Fatalf("new ingress: %v", err)
	}
	server := httptest.NewServer(newCommandMux(ingress))
	defer server.Close()

	body := []byte(`{"type":"user_message","text":"over","attachments":[]}`)
	resp := postAuthorized(t, server.URL+"/conversations/"+conv+"/commands", body)
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		t.Fatalf("expected non-201 for exhausted seq, got %d", resp.StatusCode)
	}

	// max+1 must not be allocated, persisted, or returned.
	if _, err := store.Append(context.Background(), conv, "", json.RawMessage(body)); !errors.Is(err, ErrSeqExhausted) {
		t.Fatalf("expected ErrSeqExhausted on direct append, got %v", err)
	}
	all, err := store.CatchUp(context.Background(), conv, maxJSONSafeInteger)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 || all[0].Seq != maxJSONSafeInteger {
		t.Fatalf("expected only the seed record, got %+v", all)
	}
}

func assertRejectReason(t *testing.T, r io.Reader, want RejectReason) {
	t.Helper()
	var got struct {
		Error        string `json:"error"`
		RejectReason string `json:"reject_reason"`
	}
	if err := json.NewDecoder(r).Decode(&got); err != nil {
		t.Fatalf("decode rejection: %v", err)
	}
	if got.RejectReason != string(want) {
		t.Fatalf("expected reject_reason %q, got %q", want, got.RejectReason)
	}
}
