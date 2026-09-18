// Package returnsessiontest is FIXTURE authority for tests only. It stands
// in for the browser-session adapter the account owner has not supplied
// yet. It is not Firebase, not a session verifier, and not signed-in
// acceptance; never mount it on a production server.
package returnsessiontest

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

var ErrFixtureSession = errors.New("fixture browser session is unknown or expired")

// Proof is a fixture owner-session table: an opaque cookie value → the
// human + secretary that session "proved".
type Proof struct {
	mu       sync.Mutex
	sessions map[string]returnsession.Owner
}

func NewProof() *Proof { return &Proof{sessions: map[string]returnsession.Owner{}} }

func (p *Proof) Add(cookie, humanID, personaID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessions[cookie] = returnsession.Owner{HumanID: humanID, PersonaID: personaID}
}

func (p *Proof) Remove(cookie string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, cookie)
}

// Cookie is the header name the fixture adapter reads, mirroring the real
// adapter's cookie-carried session.
const Cookie = "sumi_fixture_return_session"

func (p *Proof) OwnerClaims(_ context.Context, r *http.Request) (returnsession.Owner, error) {
	c, err := r.Cookie(Cookie)
	if err != nil {
		return returnsession.Owner{}, ErrFixtureSession
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	o, ok := p.sessions[c.Value]
	if !ok {
		return returnsession.Owner{}, ErrFixtureSession
	}
	return o, nil
}
