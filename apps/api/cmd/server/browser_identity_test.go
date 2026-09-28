package main

import (
	"context"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
)

func TestBrowserIdentityRevalidatesSessionAtAsyncCommit(t *testing.T) {
	sessions := &profileSessionAuthorizer{claims: agentevents.UserSessionClaims{UserID: "human"}, authorize: true}
	authenticate := browserIdentity(sessions, []string{testBrowserOrigin})
	identity, err := authenticate(profileRequest(`{}`))
	if err != nil || identity.HumanID != "human" {
		t.Fatal("valid authenticated mutation rejected")
	}
	sessions.authorize = false
	called := false
	if identity.Authorize(context.Background(), func(context.Context) error { called = true; return nil }) == nil || called {
		t.Fatal("revoked initiating session committed")
	}
	if _, err = authenticate(profileReadRequest()); err == nil {
		t.Fatal("revoked GET accepted")
	}
	sessions.authorize = true
	request := profileRequest(`{}`)
	request.Header.Del("X-CSRF-Token")
	if _, err = authenticate(request); err == nil {
		t.Fatal("mutation without CSRF accepted")
	}
	request = profileReadRequest()
	request.Header.Set("Origin", "https://foreign.invalid")
	if _, err = authenticate(request); err == nil {
		t.Fatal("foreign-origin read accepted")
	}
	request = profileReadRequest()
	if _, err = authenticate(request); err != nil {
		t.Fatal("same-origin browser GET without Origin rejected")
	}
}
