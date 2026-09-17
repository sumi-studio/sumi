package fileaccess

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// The full authority path: a real agentstate.Store on a real Postgres,
// the file effects registered, and a real filesvc — the same machinery a
// Cloud deployment runs. Skipped unless SUMI_TEST_DB_URL (testdb convention)
// and FILEACCESS_E2E_URL/FILEACCESS_E2E_TOKEN are set.
func e2eStore(t *testing.T) *agentstate.Store {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return agentstate.NewStore(pool)
}

// e2eTurn is one planned turn: persona + writer lease + input + plan.
type e2eTurn struct {
	personaID string
	turnID    string
	gen       int64
}

func planTurn(t *testing.T, s *agentstate.Store, personaID, turnID, inputID string, calls ...agentstate.PlanCall) e2eTurn {
	t.Helper()
	ctx := context.Background()
	lease, err := s.AcquireWriter(ctx, personaID, "e2e-holder", time.Minute)
	if err != nil {
		t.Fatalf("acquire writer: %v", err)
	}
	if _, _, err := s.SubmitInput(ctx, &agentstate.Input{PersonaID: personaID, InputID: inputID, Kind: "message",
		Payload: map[string]any{"text": "files"}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := s.LoadTurn(ctx, personaID, lease.Generation, turnID, 10); err != nil {
		t.Fatalf("load turn: %v", err)
	}
	if _, created, err := s.SavePlan(ctx, personaID, turnID, lease.Generation, 0,
		agentstate.Decision{Text: "reply", Calls: calls}); err != nil || !created {
		t.Fatalf("save plan: created=%v err=%v", created, err)
	}
	return e2eTurn{personaID: personaID, turnID: turnID, gen: lease.Generation}
}

func claim(t *testing.T, s *agentstate.Store, turn e2eTurn, tool string, callIndex int, req map[string]any) (agentstate.Operation, bool, error) {
	t.Helper()
	op, _, fresh, err := s.ClaimOperation(context.Background(), turn.personaID, turn.turnID, turn.gen,
		"op-"+turn.turnID, tool, callIndex, req)
	return op, fresh, err
}

// finishTurn commits a turn so the persona can load its next one.
func finishTurn(t *testing.T, s *agentstate.Store, turn e2eTurn, inputID string) {
	t.Helper()
	if _, err := s.CommitTurn(context.Background(), turn.personaID, turn.turnID, turn.gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events: []agentstate.EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": inputID}},
		},
	}); err != nil {
		t.Fatalf("commit turn %s: %v", turn.turnID, err)
	}
}

func e2eLedgerSetup(t *testing.T) (*agentstate.Store, *Client, string) {
	t.Helper()
	if os.Getenv("SUMI_TEST_DB_URL") == "" {
		t.Skip("SUMI_TEST_DB_URL unset; skipping Postgres integration test")
	}
	c := e2eClient(t)
	s := e2eStore(t)
	for tool, effect := range FileEffects(c) {
		if err := s.RegisterEffect(tool, effect); err != nil {
			t.Fatalf("register %s: %v", tool, err)
		}
	}
	pa := "019a0000-0000-7000-8000-0000000000c3"
	if _, _, err := s.EnsurePersona(context.Background(), pa, nil, ""); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	return s, c, pa
}

func TestE2ELedgerFileWriteReplay(t *testing.T) {
	s, c, pa := e2eLedgerSetup(t)
	ctx := context.Background()
	scope, _ := ScopeForPersona(pa)
	_ = c.Remove(ctx, scope, "ledger/file.txt", "any") // prior-run residue
	req := map[string]any{"path": "ledger/file.txt", "content_text": "ledger content"}
	turn := planTurn(t, s, pa, "t-1", "in-1",
		agentstate.PlanCall{Tool: ToolWrite, Route: "normal", Request: req})

	// Happy path: claim executes the write inside the operation transaction
	// and records the minted version as the durable receipt.
	op, fresh, err := claim(t, s, turn, ToolWrite, 0, req)
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("claim: %+v fresh=%v err=%v", op, fresh, err)
	}
	v1, _ := op.Response["version"].(float64)
	if v1 < 1 {
		t.Fatalf("no version receipt: %+v", op.Response)
	}
	got, err := c.Read(ctx, scope, "ledger/file.txt", 0, -1)
	if err != nil || string(got.Body) != "ledger content" {
		t.Fatalf("write did not land: %v %q", err, got.Body)
	}

	// Ordinary replay: same claim key replays the stored response — the
	// operation ledger short-circuits before the effect runs, so no second
	// upstream mutation is possible.
	op2, fresh2, err := claim(t, s, turn, ToolWrite, 0, req)
	if err != nil || fresh2 || op2.OperationID != op.OperationID || op2.Response["version"] != op.Response["version"] {
		t.Fatalf("replay: %+v fresh=%v err=%v", op2, fresh2, err)
	}
	st, err := c.Stat(ctx, scope, "ledger/file.txt")
	if err != nil || st.Version != int64(v1) {
		t.Fatalf("replay must not mint a second version: %+v", st)
	}
	_ = c.Remove(ctx, scope, "ledger/file.txt", "any")
}

// The interruption case the whole slice hinges on: the filesvc write
// commits, then the process dies before the operation record does — so the
// row never existed, and the retried claim runs the effect again. The
// effect's 409 -> content-compare reconcile is what preserves accepted-write
// identity: same request lands once, receipt carries the landed version.
func TestE2ELedgerWriteCrashWindow(t *testing.T) {
	s, c, pa := e2eLedgerSetup(t)
	ctx := context.Background()
	scope, _ := ScopeForPersona(pa)
	req := map[string]any{"path": "ledger/crash.txt", "content_text": "landed before ledger"}

	// Simulate the committed-external/lost-ledger state: the write lands
	// out-of-band, the operation record never committed.
	if _, err := c.Write(ctx, scope, "ledger/crash.txt", "none", []byte("landed before ledger")); err != nil {
		var se *ServiceError
		if !errors.As(err, &se) || se.Status != 409 {
			t.Fatalf("land: %v", err)
		}
	}
	landed, err := c.Stat(ctx, scope, "ledger/crash.txt")
	if err != nil {
		t.Fatal(err)
	}

	turn := planTurn(t, s, pa, "t-crash", "in-crash",
		agentstate.PlanCall{Tool: ToolWrite, Route: "normal", Request: req})
	op, fresh, err := claim(t, s, turn, ToolWrite, 0, req)
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("reconciled claim: %+v fresh=%v err=%v", op, fresh, err)
	}
	if op.Response["replayed"] != true {
		t.Fatalf("reconcile must mark replayed: %+v", op.Response)
	}
	if op.Response["version"].(float64) != float64(landed.Version) {
		t.Fatalf("receipt version %v != landed version %d", op.Response["version"], landed.Version)
	}
	// The file still holds exactly the call's bytes — one logical write.
	got, _ := c.Read(ctx, scope, "ledger/crash.txt", 0, -1)
	if string(got.Body) != "landed before ledger" {
		t.Fatalf("content: %q", got.Body)
	}
	finishTurn(t, s, turn, "in-crash")
	// deterministically — the claim reports the honest conflict and rolls
	// back, so every re-claim reports the same refusal.
	pa2 := "019a0000-0000-7000-8000-0000000000d4"
	if _, _, err := s.EnsurePersona(ctx, pa2, nil, ""); err != nil {
		t.Fatal(err)
	}
	scope2, _ := ScopeForPersona(pa2)
	if _, err := c.Write(ctx, scope2, "conflict.txt", "none", []byte("theirs")); err != nil {
		var se *ServiceError
		if !errors.As(err, &se) || se.Status != 409 {
			t.Fatal(err)
		}
	}
	turn2 := planTurn(t, s, pa2, "t-c2", "in-c2",
		agentstate.PlanCall{Tool: ToolWrite, Route: "normal", Request: map[string]any{
			"path": "conflict.txt", "content_text": "ours"}})
	_, _, err = claim(t, s, turn2, ToolWrite, 0, map[string]any{
		"path": "conflict.txt", "content_text": "ours"})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("conflict must be a deterministic claim error, got %v", err)
	}
	if got2, _ := c.Read(ctx, scope2, "conflict.txt", 0, -1); string(got2.Body) != "theirs" {
		t.Fatal("conflict clobbered content")
	}
	finishTurn(t, s, turn2, "in-c2")

	// Persona isolation through the ledger: pa2's read of pa's file is a
	// deterministic refusal — the scope never leaks.
	turn3 := planTurn(t, s, pa2, "t-c3", "in-c3",
		agentstate.PlanCall{Tool: ToolRead, Route: "normal", Request: map[string]any{"path": "ledger/crash.txt"}})
	_, _, err = claim(t, s, turn3, ToolRead, 0, map[string]any{"path": "ledger/crash.txt"})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("foreign-persona read must fail deterministically, got %v", err)
	}

	_ = c.Remove(ctx, scope, "ledger/crash.txt", "any")
	_ = c.Remove(ctx, scope, "ledger", "any")
	_ = c.Remove(ctx, scope2, "conflict.txt", "any")
}
