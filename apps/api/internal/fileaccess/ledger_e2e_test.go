package fileaccess

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
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

// F338: a diverged receipt replay surfaces through the REAL operation
// ledger as a deterministic claim failure — never a recorded success, and
// never a retryable error that would re-run the mutation. The filesvc is
// the wire-faithful fake (a real service cannot be forced into a diverged
// settle through its public API); the ledger, claim path, and error
// mapping are all real Postgres-backed machinery.
func TestE2ELedgerDivergedReceiptFailsDeterministically(t *testing.T) {
	if os.Getenv("SUMI_TEST_DB_URL") == "" {
		t.Skip("SUMI_TEST_DB_URL unset; skipping Postgres integration test")
	}
	f := newFakeFilesvc(t)
	c, _ := f.client(t)
	s := e2eStore(t)
	for tool, effect := range FileEffects(c) {
		if err := s.RegisterEffect(tool, effect); err != nil {
			t.Fatalf("register %s: %v", tool, err)
		}
	}
	pa := "019a0000-0000-7000-8000-0000000000c5"
	if _, _, err := s.EnsurePersona(context.Background(), pa, nil, ""); err != nil {
		t.Fatalf("ensure persona: %v", err)
	}
	scope, _ := ScopeForPersona(pa)

	// Seed the receipt a real filesvc writes when a keyed write settles
	// unconfirmed: foreign bytes at the path, landing unproven.
	req := map[string]any{"path": "diverged.txt", "content_text": "ours"}
	inputID := "in-div-" + fmt.Sprint(time.Now().UnixNano())
	opKey := effectOpKey(inputID + ":tool:0")
	sum := sha256.Sum256([]byte("ours"))
	f.receipts[scope+"\x00"+opKey] = fakeReceipt{
		reqHash: fakeReqHash("write", "diverged.txt", fakeIVCanon("none"), hex.EncodeToString(sum[:])),
		op:      "write", version: 4, verdict: "diverged",
	}
	// Foreign content occupies the path — the receipt must not claim it.
	f.files[scope] = map[string]fakeEntry{"diverged.txt": {body: []byte("foreign"), version: 9}}

	turn := planTurn(t, s, pa, "t-div-"+fmt.Sprint(time.Now().UnixNano()), inputID,
		agentstate.PlanCall{Tool: ToolWrite, Route: "normal", Request: req})

	_, _, err := claim(t, s, turn, ToolWrite, 0, req)
	if !errors.Is(err, agentstate.ErrBadRequest) || !strings.Contains(err.Error(), "outcome_uncertain") {
		t.Fatalf("diverged receipt must be a deterministic claim failure, got %v", err)
	}
	// The failure is durable and repeatable — the second claim answers the
	// same refusal instead of re-running or wedging on retry.
	_, _, err = claim(t, s, turn, ToolWrite, 0, req)
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("re-claim must repeat the deterministic failure, got %v", err)
	}
	// No mutation: the foreign bytes are untouched and no second version
	// was minted.
	if got := string(f.files[scope]["diverged.txt"].body); got != "foreign" {
		t.Fatalf("diverged replay touched foreign content: %q", got)
	}
	finishTurn(t, s, turn, inputID)
}

func TestE2ELedgerFileWriteReplay(t *testing.T) {
	s, c, pa := e2eLedgerSetup(t)
	ctx := context.Background()
	scope, _ := ScopeForPersona(pa)
	_ = c.Remove(ctx, scope, "ledger/file.txt", "any") // prior-run residue
	req := map[string]any{"path": "ledger/file.txt", "content_text": "ledger content"}
	turn := planTurn(t, s, pa, "t-1-"+fmt.Sprint(time.Now().UnixNano()), "in-1-"+fmt.Sprint(time.Now().UnixNano()),
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
// commits under the operation's durable identity, then the process dies
// before the operation record does — so the row never existed, and the
// retried claim runs the effect again. The service receipt — not file
// bytes — is what preserves accepted-write identity. The lost-ledger state
// is modeled by landing the identical keyed request out-of-band (the
// filesvc commit the dead process made); the API ledger genuinely has no
// operation row. An intervening independent actor then mutates the path:
// the replay must still be answered by the receipt without re-mutating.
func TestE2ELedgerWriteCrashWindow(t *testing.T) {
	s, c, pa := e2eLedgerSetup(t)
	ctx := context.Background()
	scope, _ := ScopeForPersona(pa)
	// Receipts and operation rows are durable: input IDs (and therefore
	// effect keys) must be unique per run, and crashed-run file residue
	// must not wedge create-only writes.
	runID := fmt.Sprintf("r%d", time.Now().UnixNano())
	for _, p := range []string{"ledger/crash.txt", "ledger/rm.txt"} {
		_ = c.Remove(ctx, scope, p, "any")
	}
	req := map[string]any{"path": "ledger/crash.txt", "content_text": "landed before ledger"}
	// The ledger derives the effect key as inputID:tool:callIndex; the
	// effect forwards effectOpKey(idemKey). Landing the keyed request
	// directly models "filesvc committed, API ledger rolled back".
	crashInput := "in-crash-" + runID
	opKey := effectOpKey(crashInput + ":tool:0")

	// Committed-external/lost-ledger state: the keyed write lands out of
	// band — version AND receipt are recorded upstream; no operation row.
	landedVer, _, err := c.WriteKeyed(ctx, scope, "ledger/crash.txt", "none", opKey, []byte("landed before ledger"))
	if err != nil {
		t.Fatalf("land keyed write: %v", err)
	}
	// An intervening independent actor deletes the file before the retry.
	if err := c.Remove(ctx, scope, "ledger/crash.txt", "any"); err != nil {
		t.Fatalf("intervening delete: %v", err)
	}

	turn := planTurn(t, s, pa, "t-crash-"+runID, crashInput,
		agentstate.PlanCall{Tool: ToolWrite, Route: "normal", Request: req})
	op, fresh, err := claim(t, s, turn, ToolWrite, 0, req)
	if err != nil || !fresh || op.Status != "done" {
		t.Fatalf("receipt claim: %+v fresh=%v err=%v", op, fresh, err)
	}
	if op.Response["replayed"] != true {
		t.Fatalf("claim must be answered by the receipt: %+v", op.Response)
	}
	if op.Response["version"].(float64) != float64(landedVer) {
		t.Fatalf("receipt version %v != landed version %d", op.Response["version"], landedVer)
	}
	// The replay must not recreate what the intervening op deleted.
	if _, err := c.Stat(ctx, scope, "ledger/crash.txt"); err == nil {
		t.Fatal("replayed write recreated a file another operation deleted")
	}
	finishTurn(t, s, turn, crashInput)

	// Same window for remove: land the keyed remove, another operation
	// recreates the path, the retried claim must not delete it again.
	rmReq := map[string]any{"path": "ledger/rm.txt"}
	rmInput := "in-rm-" + runID
	rmOpKey := effectOpKey(rmInput + ":tool:0")
	if _, err := c.Write(ctx, scope, "ledger/rm.txt", "none", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RemoveKeyed(ctx, scope, "ledger/rm.txt", "any", rmOpKey); err != nil {
		t.Fatalf("land keyed remove: %v", err)
	}
	if _, err := c.Write(ctx, scope, "ledger/rm.txt", "none", []byte("replacement")); err != nil {
		t.Fatalf("intervening replacement: %v", err)
	}
	rmTurn := planTurn(t, s, pa, "t-rm-"+runID, rmInput,
		agentstate.PlanCall{Tool: ToolRemove, Route: "normal", Request: rmReq})
	rmOp, fresh, err := claim(t, s, rmTurn, ToolRemove, 0, rmReq)
	if err != nil || !fresh || rmOp.Status != "done" {
		t.Fatalf("remove receipt claim: %+v fresh=%v err=%v", rmOp, fresh, err)
	}
	if rmOp.Response["replayed"] != true || rmOp.Response["removed"] != true {
		t.Fatalf("replayed remove must report the receipt: %+v", rmOp.Response)
	}
	got, err := c.Read(ctx, scope, "ledger/rm.txt", 0, -1)
	if err != nil || string(got.Body) != "replacement" {
		t.Fatalf("replayed remove deleted a later replacement: %q %v", got.Body, err)
	}
	finishTurn(t, s, rmTurn, rmInput)

	// A brand-new operation requesting bytes identical to another writer's
	// is a genuine conflict — identical content is not identity.
	if _, err := c.Write(ctx, scope, "ledger/dup.txt", "none", []byte("same")); err != nil {
		var se *ServiceError
		if !errors.As(err, &se) || se.Status != 409 {
			t.Fatalf("seed dup: %v", err)
		}
	}
	dupReq := map[string]any{"path": "ledger/dup.txt", "content_text": "same"}
	dupInput := "in-dup-" + runID
	dupTurn := planTurn(t, s, pa, "t-dup-"+runID, "in-dup-"+runID,
		agentstate.PlanCall{Tool: ToolWrite, Route: "normal", Request: dupReq})
	_, _, err = claim(t, s, dupTurn, ToolWrite, 0, dupReq)
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("new create over identical foreign bytes must conflict, got %v", err)
	}
	finishTurn(t, s, dupTurn, dupInput)

	// deterministically — the claim reports the honest conflict and rolls
	// back, so every re-claim reports the same refusal.
	pa2 := "019a0000-0000-7000-8000-0000000000d4"
	if _, _, err := s.EnsurePersona(ctx, pa2, nil, ""); err != nil {
		t.Fatal(err)
	}
	scope2, _ := ScopeForPersona(pa2)
	c2Input := "in-c2-" + runID
	if _, err := c.Write(ctx, scope2, "conflict.txt", "none", []byte("theirs")); err != nil {
		var se *ServiceError
		if !errors.As(err, &se) || se.Status != 409 {
			t.Fatal(err)
		}
	}
	turn2 := planTurn(t, s, pa2, "t-c2-"+runID, c2Input,
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
	finishTurn(t, s, turn2, c2Input)

	// Persona isolation through the ledger: pa2's read of pa's file is a
	// deterministic refusal — the scope never leaks.
	turn3 := planTurn(t, s, pa2, "t-c3-"+runID, "in-c3-"+runID,
		agentstate.PlanCall{Tool: ToolRead, Route: "normal", Request: map[string]any{"path": "ledger/crash.txt"}})
	_, _, err = claim(t, s, turn3, ToolRead, 0, map[string]any{"path": "ledger/crash.txt"})
	if !errors.Is(err, agentstate.ErrBadRequest) {
		t.Fatalf("foreign-persona read must fail deterministically, got %v", err)
	}

	_ = c.Remove(ctx, scope, "ledger/crash.txt", "any")
	_ = c.Remove(ctx, scope, "ledger/rm.txt", "any")
	_ = c.Remove(ctx, scope, "ledger/dup.txt", "any")
	_ = c.Remove(ctx, scope, "ledger", "any")
	_ = c.Remove(ctx, scope2, "conflict.txt", "any")
}
