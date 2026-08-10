package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

const profileTestHumanID = "0198f0f4-9b72-7000-8000-000000000071"

type profileSessionAuthorizer struct {
	claims       agentevents.UserSessionClaims
	verifyErr    error
	authorize    bool
	operationRun int
}

func (s *profileSessionAuthorizer) VerifySession(context.Context, string) (agentevents.UserSessionClaims, error) {
	if s.verifyErr != nil {
		return agentevents.UserSessionClaims{}, s.verifyErr
	}
	return s.claims, nil
}

func (s *profileSessionAuthorizer) AuthorizeSession(_ context.Context, _ agentevents.UserSessionClaims, operation func() error) error {
	if !s.authorize {
		return errors.New("session retired")
	}
	s.operationRun++
	return operation()
}

func profileRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/auth/profile", strings.NewReader(body))
	r.Header.Set("Origin", testBrowserOrigin)
	r.Header.Set("Content-Type", "application/json")
	csrf := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	r.Header.Set("X-CSRF-Token", csrf)
	r.AddCookie(&http.Cookie{Name: agentevents.BrowserCSRFCookie, Value: csrf})
	r.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: "signed-session"})
	return r
}

func TestHumanProfileUpdateUsesSessionHumanAndPersistsExplicitChoice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "INSERT INTO humans (human_id) VALUES ($1)", profileTestHumanID); err != nil {
		t.Fatal(err)
	}
	sessions := &profileSessionAuthorizer{
		claims: agentevents.UserSessionClaims{UserID: profileTestHumanID}, authorize: true,
	}
	server := newHumanProfileServer(koseki.New(pool), sessions, []string{testBrowserOrigin})
	request := profileRequest(`{"display_name":"  かずい\nさん  "}`)
	response := httptest.NewRecorder()
	server.serveUpdate(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("security headers = %#v", response.Header())
	}
	var result struct {
		User struct {
			ID          string `json:"id"`
			DisplayName string `json:"display_name"`
		} `json:"user"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.User.ID != profileTestHumanID || result.User.DisplayName != "かずい さん" || sessions.operationRun != 1 {
		t.Fatalf("result=%+v operationRun=%d", result, sessions.operationRun)
	}
	var stored string
	var customized bool
	if err := pool.QueryRow(ctx, "SELECT display_name, display_name_customized FROM humans WHERE human_id=$1", profileTestHumanID).Scan(&stored, &customized); err != nil {
		t.Fatal(err)
	}
	if stored != "かずい さん" || !customized {
		t.Fatalf("stored name=%q customized=%v", stored, customized)
	}

	// A client-nominated identity is never accepted; the signed session is the
	// sole subject of the update.
	request = profileRequest(`{"display_name":"Other","id":"0198f0f4-9b72-7000-8000-000000000072"}`)
	response = httptest.NewRecorder()
	server.serveUpdate(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("client-nominated Human accepted: %d %s", response.Code, response.Body.String())
	}
}

func TestHumanProfileBoundaryRejectsUnsafeRequestsAndLogoutRace(t *testing.T) {
	sessions := &profileSessionAuthorizer{
		claims: agentevents.UserSessionClaims{UserID: profileTestHumanID}, authorize: true,
	}
	server := newHumanProfileServer(koseki.New(nil), sessions, []string{testBrowserOrigin})
	tests := []struct {
		name string
		req  func() *http.Request
		want int
	}{
		{name: "missing origin", req: func() *http.Request {
			r := profileRequest(`{"display_name":"Human"}`)
			r.Header.Del("Origin")
			return r
		}, want: http.StatusForbidden},
		{name: "bad csrf", req: func() *http.Request {
			r := profileRequest(`{"display_name":"Human"}`)
			r.Header.Set("X-CSRF-Token", "bad")
			return r
		}, want: http.StatusForbidden},
		{name: "wrong content type", req: func() *http.Request {
			r := profileRequest(`{"display_name":"Human"}`)
			r.Header.Set("Content-Type", "text/plain")
			return r
		}, want: http.StatusUnsupportedMediaType},
		{name: "duplicate key", req: func() *http.Request { return profileRequest(`{"display_name":"A","display_name":"B"}`) }, want: http.StatusBadRequest},
		{name: "control", req: func() *http.Request { return profileRequest(`{"display_name":"safe\u202edanger"}`) }, want: http.StatusBadRequest},
		{name: "overlong", req: func() *http.Request {
			return profileRequest(`{"display_name":"` + strings.Repeat("名", koseki.MaxHumanDisplayNameRunes+1) + `"}`)
		}, want: http.StatusBadRequest},
		{name: "oversized", req: func() *http.Request {
			return profileRequest(`{"display_name":"` + strings.Repeat("a", maxHumanProfileRequestBytes) + `"}`)
		}, want: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			server.serveUpdate(response, test.req())
			if response.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", response.Code, test.want, response.Body.String())
			}
		})
	}

	before := sessions.operationRun
	sessions.authorize = false
	response := httptest.NewRecorder()
	server.serveUpdate(response, profileRequest(`{"display_name":"Human"}`))
	if response.Code != http.StatusUnauthorized || sessions.operationRun != before {
		t.Fatalf("logout fence status=%d operationRun=%d", response.Code, sessions.operationRun)
	}
}

func TestHumanProfileDatabaseFailureIsUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	sessions := &profileSessionAuthorizer{
		claims: agentevents.UserSessionClaims{UserID: profileTestHumanID}, authorize: true,
	}
	server := newHumanProfileServer(koseki.New(pool), sessions, []string{testBrowserOrigin})
	response := httptest.NewRecorder()
	server.serveUpdate(response, profileRequest(`{"display_name":"Human"}`))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
