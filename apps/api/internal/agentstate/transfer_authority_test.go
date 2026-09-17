package agentstate

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A transferred persona is terminally inactive: every mutation path must
// report both ErrPersonaInactive (so existing handling keeps matching) and
// ErrPersonaTransferred (so sender-facing surfaces can say the secretary
// moved). The in-flight sealed/staged authorities must NOT report moved.
func TestTransferredAuthorityCarriesMovedSentinel(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	pa := pid(t)
	mustPersona(t, s, pa)

	setAuthority := func(authority string) {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`UPDATE core_personas SET authority = $1 WHERE persona_id = $2`,
			authority, pa); err != nil {
			t.Fatalf("set authority %s: %v", authority, err)
		}
	}
	input := func(id string) *Input {
		return &Input{PersonaID: pa, InputID: id, Kind: "message",
			Payload: map[string]any{"text": "hello"}, ActorKind: "human",
			ActorID: "h-1", SourceSurface: "direct_chat", Attention: "reply"}
	}

	setAuthority("transferred")
	if _, _, err := s.SubmitInput(ctx, input("in-moved")); !errors.Is(err, ErrPersonaInactive) ||
		!errors.Is(err, ErrPersonaTransferred) {
		t.Fatalf("transferred submit: err=%v, want ErrPersonaInactive+ErrPersonaTransferred", err)
	}
	if got, err := s.PersonaAuthority(ctx, pa); err != nil || got != "transferred" {
		t.Fatalf("PersonaAuthority: %q, %v", got, err)
	}
	hid := pid(t)
	if _, err := pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, hid); err != nil {
		t.Fatalf("insert human: %v", err)
	}
	if _, err := s.BindHuman(ctx, pa, hid); !errors.Is(err, ErrPersonaInactive) ||
		!errors.Is(err, ErrPersonaTransferred) {
		t.Fatalf("transferred bind: err=%v", err)
	}
	if _, err := s.AcquireWriter(ctx, pa, "h", time.Minute); !errors.Is(err, ErrPersonaInactive) ||
		!errors.Is(err, ErrPersonaTransferred) {
		t.Fatalf("transferred acquire: err=%v", err)
	}

	for _, authority := range []string{"sealed", "staged"} {
		setAuthority(authority)
		if _, _, err := s.SubmitInput(ctx, input("in-"+authority)); !errors.Is(err, ErrPersonaInactive) ||
			errors.Is(err, ErrPersonaTransferred) {
			t.Fatalf("%s submit must not report moved: err=%v", authority, err)
		}
	}

	// Back on active the persona accepts work again — a cancelled transfer
	// restores ordinary admission rather than leaving a moved residue.
	setAuthority("active")
	if _, created, err := s.SubmitInput(ctx, input("in-back")); err != nil || !created {
		t.Fatalf("active submit after restore: created=%v err=%v", created, err)
	}
}
