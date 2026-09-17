package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

const (
	testSecret   = "test-admin-secret-0123456789abcdef"
	testPersonaA = "01930e00-0000-7000-8000-00000000000a"
	testPersonaB = "01930e00-0000-7000-8000-00000000000b"
)

func newTestFM() *fmServer {
	return &fmServer{secret: []byte(testSecret)}
}

// coreToken mirrors agentstate.Server.PersonaToken — a core_ persona token
// must not open the fm surface.
func coreTokenFor(personaID string) string {
	return agentstate.NewServer(nil, testSecret).PersonaToken(personaID)
}

func fmReq(token, persona string) *http.Request {
	r := httptest.NewRequest("GET", "/fm/"+persona+"/state", nil)
	r.SetPathValue("persona", persona)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

// The fm_/core_/admin credential boundary is the security-relevant property
// of this slice: fm_ is scoped to one persona's browser surface, core_ is
// for the secretary core's internal routes, admin is accepted on both.
func TestFMAuthMatrix(t *testing.T) {
	s := newTestFM()
	fmA := s.fmToken(testPersonaA)
	fmB := s.fmToken(testPersonaB)
	coreA := coreTokenFor(testPersonaA)

	cases := []struct {
		name    string
		token   string
		persona string
		want    bool
	}{
		{"fm token for its persona", fmA, testPersonaA, true},
		{"fm token for another persona", fmA, testPersonaB, false},
		{"fm token B on B", fmB, testPersonaB, true},
		{"admin on fm surface", testSecret, testPersonaA, true},
		{"core token is not a browser token", coreA, testPersonaA, false},
		{"no token", "", testPersonaA, false},
		{"garbage token", "fm_garbage", testPersonaA, false},
		{"fm-shaped token with wrong hmac", "fm_" + fmB[3:], testPersonaA, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := fmReq(tc.token, tc.persona)
			w := httptest.NewRecorder()
			got, ok := s.scope(w, r)
			if ok != tc.want {
				t.Fatalf("scope()=%v want %v (status %d)", ok, tc.want, w.Code)
			}
			if tc.want && got != tc.persona {
				t.Fatalf("persona %q want %q", got, tc.persona)
			}
			if !tc.want && w.Code != http.StatusUnauthorized {
				t.Fatalf("rejected request status %d, want 401", w.Code)
			}
		})
	}
}

func TestScopeRejectsMalformedPersona(t *testing.T) {
	s := newTestFM()
	r := fmReq(testSecret, "not-a-uuid")
	w := httptest.NewRecorder()
	if _, ok := s.scope(w, r); ok {
		t.Fatal("malformed persona accepted")
	}
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", w.Code)
	}
}

// A replayed input_id carrying a different request is a caller conflict
// (409), matching how the agentstate server maps ErrTurnConflict — not a
// 500, which would look like an internal failure. Bad input stays 400 and
// unrelated store failures stay 500.
func TestSubmitErrorStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"bad request", agentstate.ErrBadRequest, http.StatusBadRequest},
		{"turn conflict", agentstate.ErrTurnConflict, http.StatusConflict},
		{"wrapped turn conflict",
			fmt.Errorf("%w: input_id replay carries a different request", agentstate.ErrTurnConflict),
			http.StatusConflict},
		{"internal failure", errors.New("database connection lost"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := submitErrorStatus(tc.err); got != tc.want {
				t.Fatalf("submitErrorStatus=%d want %d", got, tc.want)
			}
		})
	}
}
