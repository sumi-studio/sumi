package koseki

// Registration transfer tests drive the REAL account transaction
// (ResolveAuthProof + ConfirmAuthFlow) against a transfer session whose
// import was staged by the real portable seal→export→upload path on a second
// database standing in for the Local placement. Nothing here stubs the claim
// or the account insert; only the "source placement" is a test database.

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

// localPlacement is a second migrated database playing the Local placement:
// its own agent state, portable service and persona.
type localPlacement struct {
	pool  *pgxpool.Pool
	state *agentstate.Store
	svc   *portable.Service
	pid   string
}

func newLocalPlacement(t *testing.T) localPlacement {
	t.Helper()
	ctx := context.Background()
	pool := testdb.Create(t)
	if err := db.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate local placement: %v", err)
	}
	p := localPlacement{pool: pool, state: agentstate.NewStore(pool), svc: portable.NewService(pool)}
	p.pid = uuid.Must(uuid.NewV7()).String()
	if _, _, err := p.state.EnsurePersona(ctx, p.pid, nil, "Local secretary"); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	if _, _, err := p.state.SubmitInput(ctx, &agentstate.Input{
		PersonaID: p.pid, InputID: "in-carried", Kind: "message",
		Payload: map[string]any{"text": "明日 9 時に会議"}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
	}); err != nil {
		t.Fatalf("carried input: %v", err)
	}
	return p
}

// stageTransfer runs the real move: create the session on the destination,
// bind this placement as source, seal, export, upload → staged.
func stageTransfer(t *testing.T, ctx context.Context, cloud *pgxpool.Pool, sessions *transfersession.Service, local localPlacement, uid string) string {
	t.Helper()
	created, _, err := sessions.Create(ctx, transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: uid})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	sessionID := created.View.SessionID
	own, err := local.svc.PlacementID(ctx)
	if err != nil {
		t.Fatalf("local placement id: %v", err)
	}
	if _, err := sessions.BindSource(ctx, sessionID, created.Grant,
		transfersession.Source{PlacementID: own, PersonaID: local.pid}); err != nil {
		t.Fatalf("bind source: %v", err)
	}
	dest, err := portable.NewService(cloud).PlacementID(ctx)
	if err != nil {
		t.Fatalf("cloud placement id: %v", err)
	}
	if _, err := local.svc.Seal(ctx, local.pid, sessionID, dest); err != nil {
		t.Fatalf("seal: %v", err)
	}
	var buf bytes.Buffer
	if _, err := local.svc.Export(ctx, local.pid, sessionID, &buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	v, _, err := sessions.Upload(ctx, sessionID, created.Grant, &buf)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if v.Status != transfersession.StatusStaged {
		t.Fatalf("session after upload: %+v", v)
	}
	return sessionID
}

func transferStore(t *testing.T) (*Store, *transfersession.Service, context.Context) {
	t.Helper()
	store, ctx := authFlowStore(t)
	sessions := transfersession.New(store.pool, transfersession.Config{})
	store.Transfers = sessions
	return store, sessions, ctx
}

func startRegistration(t *testing.T, ctx context.Context, store *Store, uid, email string) (AuthFlow, string) {
	t.Helper()
	nonce := testNonce(t)
	flow := startEmailFlow(t, ctx, store, IntentSignIn, email, nonce)
	pending, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof(uid, email))
	if err != nil {
		t.Fatalf("resolve proof: %v", err)
	}
	if pending.Status != "confirmation_required" || pending.ConfirmationAction != ActionCreateAccount {
		t.Fatalf("unexpected pending: %+v", pending)
	}
	return pending, nonce
}

func TestRegistrationClaimsTheCarriedSecretary(t *testing.T) {
	store, sessions, ctx := transferStore(t)
	local := newLocalPlacement(t)
	uid := "moved-" + uuid.Must(uuid.NewV7()).String()[:12]
	sessionID := stageTransfer(t, ctx, store.pool, sessions, local, uid)

	flow, nonce := startRegistration(t, ctx, store, uid, "moved@example.com")
	result, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount)
	if err != nil {
		t.Fatalf("confirm with staged transfer: %v", err)
	}
	if result.TerminalOutcome != OutcomeAccountCreated || result.AgentID != local.pid {
		t.Fatalf("account did not take the carried persona: %+v (carried %s)", result, local.pid)
	}
	assertRegistryCounts(t, ctx, store, 1, 1)
	assertEnabledDirectChatInstallation(t, ctx, store, result.HumanID)

	// The carried persona is bound to the new human and activated on Cloud by
	// the post-commit reconcile; the session answers activated and the same
	// secretary state is readable under the same identity.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var status, authority string
		var boundHuman *string
		if err := store.pool.QueryRow(ctx,
			`SELECT status FROM transfer_sessions WHERE session_id = $1`, sessionID).Scan(&status); err != nil {
			t.Fatalf("session status: %v", err)
		}
		if err := store.pool.QueryRow(ctx,
			`SELECT human_id::text, authority FROM core_personas WHERE persona_id = $1`, local.pid).Scan(&boundHuman, &authority); err != nil {
			t.Fatalf("carried persona: %v", err)
		}
		if status == transfersession.StatusActivated && authority == "active" {
			if boundHuman == nil || *boundHuman != result.HumanID {
				t.Fatalf("carried persona bound to %v, want %s", boundHuman, result.HumanID)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("activation never committed: session %s authority %s", status, authority)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The Local placement's copy stayed sealed after the move and the carried
	// state arrived under the same persona identity.
	var localAuthority string
	if err := local.pool.QueryRow(ctx,
		`SELECT authority FROM core_personas WHERE persona_id = $1`, local.pid).Scan(&localAuthority); err != nil {
		t.Fatalf("local persona: %v", err)
	}
	if localAuthority != "sealed" {
		t.Fatalf("local authority = %q, want sealed", localAuthority)
	}
	var carried int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-carried'`, local.pid).Scan(&carried); err != nil {
		t.Fatalf("carried inputs: %v", err)
	}
	if carried != 1 {
		t.Fatalf("carried input missing on Cloud: %d", carried)
	}
}

func TestRegistrationWithAwaitingTransferNeverMintsASecondSecretary(t *testing.T) {
	store, sessions, ctx := transferStore(t)
	uid := "pending-" + uuid.Must(uuid.NewV7()).String()[:12]
	if _, _, err := sessions.Create(ctx, transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: uid}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	flow, nonce := startRegistration(t, ctx, store, uid, "pending@example.com")
	if _, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount); !errors.Is(err, transfersession.ErrPending) {
		t.Fatalf("awaiting-bundle claim: %v", err)
	}
	assertRegistryCounts(t, ctx, store, 0, 0)
}

func TestRegistrationAfterCancelledTransferMintsFreshSecretary(t *testing.T) {
	store, sessions, ctx := transferStore(t)
	uid := "cancelled-" + uuid.Must(uuid.NewV7()).String()[:12]
	created, _, err := sessions.Create(ctx, transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: uid})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := sessions.CancelBySubject(ctx, created.View.SessionID,
		transfersession.Subject{Provider: transfersession.ProviderFirebase, Subject: uid}, "awaiting_bundle"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	flow, nonce := startRegistration(t, ctx, store, uid, "cancelled@example.com")
	result, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount)
	if err != nil {
		t.Fatalf("confirm after cancel: %v", err)
	}
	if result.TerminalOutcome != OutcomeAccountCreated || result.AgentID == "" {
		t.Fatalf("fresh registration failed: %+v", result)
	}
	assertRegistryCounts(t, ctx, store, 1, 1)
}

func TestRegistrationDoesNotAdoptAnotherCredentialSession(t *testing.T) {
	store, sessions, ctx := transferStore(t)
	local := newLocalPlacement(t)
	owner := "owner-" + uuid.Must(uuid.NewV7()).String()[:12]
	other := "other-" + uuid.Must(uuid.NewV7()).String()[:12]
	sessionID := stageTransfer(t, ctx, store.pool, sessions, local, owner)

	// A different credential's registration sees no claim — the staged
	// session belongs to its own subject and must stay staged for it.
	flow, nonce := startRegistration(t, ctx, store, other, "other@example.com")
	result, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount)
	if err != nil {
		t.Fatalf("other credential registration: %v", err)
	}
	if result.AgentID == local.pid {
		t.Fatalf("another credential adopted the carried persona")
	}
	var status string
	if err := store.pool.QueryRow(ctx,
		`SELECT status FROM transfer_sessions WHERE session_id = $1`, sessionID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != transfersession.StatusStaged {
		t.Fatalf("the owner's session was touched: %s", status)
	}
}

func TestRegistrantProofSubject(t *testing.T) {
	store, ctx := authFlowStore(t)
	uid := "proof-" + uuid.Must(uuid.NewV7()).String()[:12]

	t.Run("live create-account confirmation proves the verified subject", func(t *testing.T) {
		flow, nonce := startRegistration(t, ctx, store, uid, "proof@example.com")
		got, epoch, err := store.RegistrantProofSubject(ctx, flow.FlowID, nonce)
		if err != nil {
			t.Fatalf("proof: %v", err)
		}
		if got != uid || epoch != flow.BrowserEpochHash {
			t.Fatalf("subject %q epoch %q, want %q %q", got, epoch, uid, flow.BrowserEpochHash)
		}
		if _, _, err := store.RegistrantProofSubject(ctx, flow.FlowID, testNonce(t)); !errors.Is(err, ErrAuthProofMismatch) {
			t.Fatalf("wrong nonce: %v", err)
		}
	})

	t.Run("completed account creation proves inside the replay window", func(t *testing.T) {
		done := "done-" + uuid.Must(uuid.NewV7()).String()[:12]
		flow, nonce := startRegistration(t, ctx, store, done, "done@example.com")
		if _, err := store.ConfirmAuthFlow(ctx, flow.FlowID, nonce, ActionCreateAccount); err != nil {
			t.Fatalf("confirm: %v", err)
		}
		got, _, err := store.RegistrantProofSubject(ctx, flow.FlowID, nonce)
		if err != nil || got != done {
			t.Fatalf("replay proof: %q %v", got, err)
		}
	})

	t.Run("a sign-in outcome proves nothing", func(t *testing.T) {
		existing := "existing-" + uuid.Must(uuid.NewV7()).String()[:12]
		if _, err := store.AutoRegister(ctx, "firebase", existing); err != nil {
			t.Fatal(err)
		}
		nonce := testNonce(t)
		flow := startEmailFlow(t, ctx, store, IntentSignIn, "existing@example.com", nonce)
		if _, err := store.ResolveAuthProof(ctx, flow.FlowID, nonce, emailProof(existing, "existing@example.com")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.RegistrantProofSubject(ctx, flow.FlowID, nonce); !errors.Is(err, ErrAuthFlowConsumed) {
			t.Fatalf("sign-in flow proved: %v", err)
		}
	})

	t.Run("closed and unknown flows prove nothing", func(t *testing.T) {
		nonce := testNonce(t)
		if _, _, err := store.RegistrantProofSubject(ctx, uuid.Must(uuid.NewV7()).String(), nonce); !errors.Is(err, ErrInvalidAuthFlow) {
			t.Fatalf("unknown flow: %v", err)
		}
	})
}
