package portable

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// placement is one isolated database: a Local or a Cloud, in these tests.
type placement struct {
	pool  *pgxpool.Pool
	state *agentstate.Store
	svc   *Service
}

func newPlacement(t *testing.T) placement {
	t.Helper()
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return placement{pool: pool, state: agentstate.NewStore(pool), svc: NewService(pool)}
}

func newID(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

// must fails the running test on an unexpected error. It panics because Go
// cannot pass a two-value call alongside *testing.T; the testing package
// reports the panic as that test's failure.
func must[T any](v T, err error) T {
	if err != nil {
		panic(fmt.Sprintf("unexpected error: %v", err))
	}
	return v
}

func drop[T any](v T, _ bool, err error) (T, error) { return v, err }

func submit(t *testing.T, p placement, personaID, inputID, text string) {
	t.Helper()
	_, _, err := p.state.SubmitInput(context.Background(), &agentstate.Input{
		PersonaID: personaID, InputID: inputID, Kind: "message",
		Payload: map[string]any{"text": text}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
	})
	if err != nil {
		t.Fatalf("submit %s: %v", inputID, err)
	}
}

func note(text string) agentstate.PlanCall {
	return agentstate.PlanCall{Tool: "journal.note", Request: map[string]any{"text": text}}
}

// liveSecretary builds representative state through the real state service:
// a completed turn that wrote a memory note and set a reminder, a turn that
// crashed after executing the first of its two recorded calls, and a queued
// message. It returns the writer generation that was running.
func liveSecretary(t *testing.T, p placement, personaID string) int64 {
	t.Helper()
	ctx := context.Background()
	must(drop(p.state.EnsurePersona(ctx, personaID, nil, "Local secretary")))
	submit(t, p, personaID, "in-1", "please remember I take tea at 15:00")
	gen := must(p.state.AcquireWriter(ctx, personaID, "local-core", time.Minute)).Generation
	must(p.state.Recover(ctx, personaID, gen))

	load := must(p.state.LoadTurn(ctx, personaID, gen, "turn-1", 50))
	if load.Input == nil || load.Input.InputID != "in-1" {
		t.Fatalf("turn-1 claimed %+v", load.Input)
	}
	reminder := agentstate.PlanCall{Tool: "schedule.set", Request: map[string]any{
		"schedule_id": "sch-tea",
		"wake_at":     time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano),
		"payload":     map[string]any{"text": "tea time"},
	}}
	plan1 := agentstate.Decision{Text: "noted", Calls: []agentstate.PlanCall{note("The user takes tea at 15:00"), reminder}}
	must(drop(p.state.SavePlan(ctx, personaID, "turn-1", gen, 0, plan1)))
	for i, c := range plan1.Calls {
		if _, _, err := p.state.ClaimOperation(ctx, personaID, "turn-1", gen, fmt.Sprintf("op-1-%d", i), c.Tool, i, c.Request); err != nil {
			t.Fatalf("claim turn-1 call %d: %v", i, err)
		}
	}
	must(p.state.CommitTurn(ctx, personaID, "turn-1", gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events:  []agentstate.EventInput{{Kind: "assistant_message", Payload: map[string]any{"text": "noted"}}},
		Output:  map[string]any{"text": "noted"},
	}))

	submit(t, p, personaID, "in-2", "we are moving to the cloud")
	load = must(p.state.LoadTurn(ctx, personaID, gen, "turn-2", 50))
	if load.Input == nil || load.Input.InputID != "in-2" {
		t.Fatalf("turn-2 claimed %+v", load.Input)
	}
	plan2 := agentstate.Decision{Text: "moving", Calls: []agentstate.PlanCall{note("Mid-move note"), note("Second note after the move")}}
	must(drop(p.state.SavePlan(ctx, personaID, "turn-2", gen, 0, plan2)))
	if _, _, err := p.state.ClaimOperation(ctx, personaID, "turn-2", gen, "op-2-a", "journal.note", 0, plan2.Calls[0].Request); err != nil {
		t.Fatalf("claim turn-2 call 0: %v", err)
	}
	// The process dies here: call 1 never runs and the turn never commits.

	submit(t, p, personaID, "in-3", "and then check my calendar")
	return gen
}

func exportBytes(t *testing.T, p placement, personaID, transferID string) ([]byte, Receipt) {
	t.Helper()
	var buf bytes.Buffer
	rec, err := p.svc.Export(context.Background(), personaID, transferID, &buf)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	return buf.Bytes(), rec
}

func rowLines(t *testing.T, p placement, personaID string) []byte {
	t.Helper()
	ctx := context.Background()
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL TimeZone = 'UTC'`); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := writeRows(ctx, tx, personaID, &buf); err != nil {
		t.Fatalf("write rows: %v", err)
	}
	return buf.Bytes()
}

func notesWithText(t *testing.T, p placement, personaID, text string) int {
	t.Helper()
	n := 0
	for _, e := range must(p.state.Events(context.Background(), personaID, 0, 1000)) {
		if e.Kind == "note" && e.Payload["text"] == text {
			n++
		}
	}
	return n
}

func authority(t *testing.T, p placement, personaID string) string {
	t.Helper()
	return must(p.state.PersonaState(context.Background(), personaID)).Persona.Authority
}

// bundleHeader decodes the first line of an exported bundle.
func bundleHeader(t *testing.T, bundle []byte) Header {
	t.Helper()
	var hdr Header
	if err := strictDecode(bundle[:bytes.IndexByte(bundle, '\n')], &hdr); err != nil {
		t.Fatalf("bundle header: %v", err)
	}
	return hdr
}

// placementID is the durable identity a seal addresses a bundle to.
func placementID(t *testing.T, p placement) string {
	t.Helper()
	return must(p.svc.PlacementID(context.Background()))
}

// retireDest retires the transfer on the destination and returns the proof
// the source's abort requires.
func retireDest(t *testing.T, dest placement, personaID, transferID, key string) Receipt {
	t.Helper()
	return must(dest.svc.Retire(context.Background(), personaID, transferID, placementID(t, dest), key))
}

// The same secretary moves from one database to another and continues: its
// journal and memories arrive byte for byte, the source is fenced at the
// seal, staging has no execution authority, and after activation the
// destination recovers the interrupted turn by continuing its recorded plan
// — the note already written before the move is not written again.
func TestTransferContinuesTheSameSecretary(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	localGen := liveSecretary(t, local, pid)
	cloudID := must(cloud.svc.PlacementID(ctx))
	if other := must(cloud.svc.PlacementID(ctx)); other != cloudID {
		t.Fatalf("placement id is not stable: %s then %s", cloudID, other)
	}

	rec := must(local.svc.Seal(ctx, pid, "move-0001", cloudID))
	if rec.DestinationID != cloudID {
		t.Fatalf("seal receipt addressed to %s, want %s", rec.DestinationID, cloudID)
	}
	if rec.Cut.GenerationHighWater != localGen+1 {
		t.Fatalf("epoch = %d, want %d", rec.Cut.GenerationHighWater, localGen+1)
	}
	want := Continuity{JournalEvents: 3, Notes: 2, QueuedInputs: 1, ClaimedInputs: 1, RunningTurns: 1,
		UnfinishedPlans: 1, PendingSchedules: 1, UndeliveredOut: 1}
	if rec.Continuity != want {
		t.Fatalf("continuity = %+v, want %+v", rec.Continuity, want)
	}
	if again := must(local.svc.Seal(ctx, pid, "move-0001", cloudID)); !again.SealedAt.Equal(rec.SealedAt) {
		t.Fatalf("seal replay produced a new cut: %v vs %v", again.SealedAt, rec.SealedAt)
	}
	if _, err := local.svc.Seal(ctx, pid, "move-0001", newID(t)); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("seal replay for a different destination: %v, want conflict", err)
	}

	// The running local writer is fenced; nothing new starts on the source.
	if _, err := local.state.CommitTurn(ctx, pid, "turn-2", localGen, agentstate.CommitRequest{Outcome: "complete"}); !errors.Is(err, agentstate.ErrGenerationFence) {
		t.Fatalf("source commit after seal: %v, want fenced", err)
	}
	if _, err := local.state.AcquireWriter(ctx, pid, "another-core", time.Minute); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("source acquire after seal: %v, want inactive", err)
	}
	_, _, err := local.state.SubmitInput(ctx, &agentstate.Input{PersonaID: pid, InputID: "in-late", Kind: "message", Payload: map[string]any{"text": "late"}})
	if !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("new input after seal: %v, want inactive", err)
	}
	if _, created, err := local.state.SubmitInput(ctx, &agentstate.Input{
		PersonaID: pid, InputID: "in-3", Kind: "message", Payload: map[string]any{"text": "and then check my calendar"},
		ActorKind: "human", ActorID: "owner", SourceSurface: "test",
	}); err != nil || created {
		t.Fatalf("replayed accepted input after seal: created=%v err=%v", created, err)
	}

	bundle, exported := exportBytes(t, local, pid, "move-0001")
	if second, _ := exportBytes(t, local, pid, "move-0001"); !bytes.Equal(bundle, second) {
		t.Fatal("re-export is not byte-identical")
	}
	if strings.Contains(string(bundle), "local-core") {
		t.Fatal("bundle carries the source lease holder")
	}

	humanID := newID(t)
	if _, err := cloud.pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, humanID); err != nil {
		t.Fatal(err)
	}
	staged, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID)
	if err != nil || !created {
		t.Fatalf("import: created=%v err=%v", created, err)
	}
	if staged.ContentSHA256 != exported.ContentSHA256 || staged.Status != "staged" || staged.Continuity != want {
		t.Fatalf("staged receipt %+v does not match export %+v", staged, exported)
	}
	if !bytes.Equal(rowLines(t, local, pid), rowLines(t, cloud, pid)) {
		t.Fatal("destination rows differ from source rows")
	}
	st := must(cloud.state.PersonaState(ctx, pid))
	if st.Persona.HumanID == nil || *st.Persona.HumanID != humanID || st.Persona.DisplayName != "Local secretary" {
		t.Fatalf("destination persona %+v", st.Persona)
	}

	// Staging runs nothing, even for a caller presenting the carried epoch.
	if _, err := cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("staged acquire: %v, want inactive", err)
	}
	if _, err := cloud.state.CommitTurn(ctx, pid, "turn-2", rec.Cut.GenerationHighWater, agentstate.CommitRequest{Outcome: "complete"}); !errors.Is(err, agentstate.ErrGenerationFence) {
		t.Fatalf("staged commit under the epoch: %v, want fenced", err)
	}
	if _, _, err := cloud.state.SubmitInput(ctx, &agentstate.Input{PersonaID: pid, InputID: "in-cloud", Kind: "message", Payload: map[string]any{}}); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("staged input: %v, want inactive", err)
	}
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID); err != nil || created {
		t.Fatalf("import replay: created=%v err=%v", created, err)
	}

	if _, err := local.svc.Complete(ctx, pid, "move-0001", strings.Repeat("0", 64)); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("complete with a foreign proof: %v", err)
	}
	act := must(cloud.svc.Activate(ctx, pid, "move-0001"))
	if act.ActivateProof == "" {
		t.Fatal("activation produced no proof")
	}
	if again := must(cloud.svc.Activate(ctx, pid, "move-0001")); again.ActivateProof != act.ActivateProof {
		t.Fatal("activate replay produced a different proof")
	}
	// The same proof is discoverable from the destination's status after a
	// lost activate response.
	ist := must(cloud.svc.Status(ctx, "import", "move-0001"))
	if ist.ActivateProof != act.ActivateProof {
		t.Fatal("status does not return the activation proof")
	}
	must(local.svc.Complete(ctx, pid, "move-0001", ist.ActivateProof))
	must(local.svc.Complete(ctx, pid, "move-0001", ist.ActivateProof))
	if a := authority(t, local, pid); a != "transferred" {
		t.Fatalf("source authority %s", a)
	}

	// The destination's first writer continues the life.
	gen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	if gen != rec.Cut.GenerationHighWater+1 {
		t.Fatalf("destination generation %d, want %d", gen, rec.Cut.GenerationHighWater+1)
	}
	recovered := must(cloud.state.Recover(ctx, pid, gen))
	if len(recovered.InterruptedTurns) != 1 || recovered.InterruptedTurns[0] != "turn-2" ||
		len(recovered.RequeuedInputs) != 1 || recovered.RequeuedInputs[0] != "in-2" {
		t.Fatalf("recovery %+v", recovered)
	}
	load := must(cloud.state.LoadTurn(ctx, pid, gen, "turn-2b", 50))
	if load.Input == nil || load.Input.InputID != "in-2" || load.Turn.Attempt != 2 ||
		load.Plan == nil || len(load.Plan.Plan) != 1 || len(load.Plan.Plan[0].Calls) != 2 {
		t.Fatalf("resumed turn %+v input %+v plan %+v", load.Turn, load.Input, load.Plan)
	}
	op, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-2b", gen, "op-2b-a", "journal.note", 0, load.Plan.Plan[0].Calls[0].Request)
	if err != nil || fresh || op.OperationID != "op-2-a" {
		t.Fatalf("carried call 0: op=%+v fresh=%v err=%v", op, fresh, err)
	}
	if _, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-2b", gen, "op-2b-b", "journal.note", 1, load.Plan.Plan[0].Calls[1].Request); err != nil || !fresh {
		t.Fatalf("remaining call 1: fresh=%v err=%v", fresh, err)
	}
	must(cloud.state.CommitTurn(ctx, pid, "turn-2b", gen, agentstate.CommitRequest{Outcome: "complete", Output: map[string]any{"text": "moving"}}))

	load = must(cloud.state.LoadTurn(ctx, pid, gen, "turn-3", 50))
	if load.Input == nil || load.Input.InputID != "in-3" {
		t.Fatalf("queued input after the move: %+v", load.Input)
	}
	must(cloud.state.CommitTurn(ctx, pid, "turn-3", gen, agentstate.CommitRequest{Outcome: "complete", Output: map[string]any{}}))
	fired := must(cloud.state.DispatchDueSchedules(ctx, pid, gen, time.Now(), 10))
	if len(fired) != 1 || fired[0].ScheduleID != "sch-tea" {
		t.Fatalf("carried reminder fired %+v", fired)
	}
	load = must(cloud.state.LoadTurn(ctx, pid, gen, "turn-wake", 50))
	if load.Input == nil || load.Input.InputID != "sched:sch-tea" {
		t.Fatalf("wake input %+v", load.Input)
	}

	for text, n := range map[string]int{"The user takes tea at 15:00": 1, "Mid-move note": 1, "Second note after the move": 1} {
		if got := notesWithText(t, cloud, pid, text); got != n {
			t.Fatalf("note %q appears %d times on the destination, want %d", text, got, n)
		}
	}
	after := must(cloud.state.Events(ctx, pid, rec.Cut.LatestEventSeq, 100))
	if len(after) == 0 || after[0].Seq != rec.Cut.LatestEventSeq+1 {
		t.Fatalf("journal does not continue after the cut: %+v", after)
	}
	if _, err := local.state.AcquireWriter(ctx, pid, "local-core", time.Minute); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("source acquire after completion: %v", err)
	}
}

func mutateLines(bundle []byte, fn func(lines []string) []string) []byte {
	lines := strings.SplitAfter(string(bundle), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return []byte(strings.Join(fn(lines), ""))
}

func TestImportRefusesDamagedOrUnsupportedBundles(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	cloudID := must(cloud.svc.PlacementID(ctx))
	must(local.svc.Seal(ctx, pid, "move-0002", cloudID))
	bundle, _ := exportBytes(t, local, pid, "move-0002")
	firstLine := bundle[:bytes.IndexByte(bundle, '\n')+1]

	cases := map[string]struct {
		bundle []byte
		want   string
	}{
		"cut before the trailer": {mutateLines(bundle, func(l []string) []string { return l[:len(l)-1] }), "truncated"},
		"cut mid-line":           {bundle[:len(bundle)-40], "truncated"},
		"one changed byte": {bytes.Replace(bundle, []byte("tea at 15:00"), []byte("tea at 16:00"), 1),
			"digest mismatch"},
		"future format version": {bytes.Replace(bundle, []byte(`"format_version":1`), []byte(`"format_version":2`), 1),
			"format version 2"},
		"undeclared extension section": {bytes.Replace(bundle, []byte(`"sections":[{"name":"core","contract":"core.v1"}]`),
			[]byte(`"sections":[{"name":"core","contract":"core.v1"},{"name":"files","contract":"files.v1"}]`), 1), "sections"},
		"unknown column": {mutateLines(bundle, func(l []string) []string {
			for i, line := range l {
				if strings.Contains(line, `"table":"core_events"`) {
					l[i] = strings.Replace(line, `"data":{`, `"data":{"mood": "calm", `, 1)
					break
				}
			}
			return l
		}), "columns"},
		"data after the trailer": {append(append([]byte{}, bundle...), firstLine...), "after the trailer"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := cloud.svc.Import(ctx, bytes.NewReader(tc.bundle), nil)
			if !errors.Is(err, ErrBadBundle) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("import err = %v, want ErrBadBundle mentioning %q", err, tc.want)
			}
			if _, err := cloud.state.PersonaState(ctx, pid); !errors.Is(err, agentstate.ErrPersonaNotFound) {
				t.Fatalf("a refused bundle left a persona behind: %v", err)
			}
			if _, err := cloud.svc.Status(ctx, "import", "move-0002"); !errors.Is(err, ErrTransferNotFound) {
				t.Fatalf("a refused bundle left a ledger row: %v", err)
			}
		})
	}
	// The undamaged bundle still imports after all of that.
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil); err != nil || !created {
		t.Fatalf("clean import after refusals: created=%v err=%v", created, err)
	}
}

// Reference integrity is checked against the rows themselves, not only the
// digest: a bundle with a valid digest whose operation no longer matches its
// recorded plan is refused, because the destination could otherwise run the
// effect again as new work.
func TestImportRefusesBrokenReferences(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-0003", must(cloud.svc.PlacementID(ctx))))
	bundle, _ := exportBytes(t, local, pid, "move-0003")

	var out bytes.Buffer
	h := sha256.New()
	sc := bufio.NewScanner(bytes.NewReader(bundle))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		line := sc.Text() + "\n"
		switch {
		case strings.Contains(line, `"record":"trailer"`):
			var tr Trailer
			if err := json.Unmarshal([]byte(line), &tr); err != nil {
				t.Fatal(err)
			}
			tr.ContentSHA256 = hex.EncodeToString(h.Sum(nil))
			raw, _ := json.Marshal(tr)
			out.Write(append(raw, '\n'))
			continue
		case strings.Contains(line, `"table":"core_operations"`) && strings.Contains(line, `"op-2-a"`):
			line = strings.Replace(line, `in-2:tool:0`, `in-2:tool:7`, 1)
		}
		h.Write([]byte(line))
		out.WriteString(line)
	}
	_, _, err := cloud.svc.Import(ctx, bytes.NewReader(out.Bytes()), nil)
	if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), "operation_outside_recorded_plan") {
		t.Fatalf("import err = %v, want the plan reference violation", err)
	}
	if _, err := cloud.state.PersonaState(ctx, pid); !errors.Is(err, agentstate.ErrPersonaNotFound) {
		t.Fatalf("refused import left a persona: %v", err)
	}
}

func TestSealRefusesUnresolvedExternalOperation(t *testing.T) {
	ctx := context.Background()
	local := newPlacement(t)
	pid := newID(t)
	gen := liveSecretary(t, local, pid)
	// A future external tool left a claimed effect whose result is unknown.
	if _, err := local.pool.Exec(ctx, `
		INSERT INTO core_operations (persona_id, operation_id, turn_id, tool, idempotency_key, request, status, claimed_generation)
		VALUES ($1, 'op-mail', 'turn-2', 'mail.send', 'in-2:tool:9', '{}', 'running', $2)`, pid, gen); err != nil {
		t.Fatal(err)
	}
	if _, err := local.svc.Seal(ctx, pid, "move-0004", newID(t)); !errors.Is(err, ErrUnresolvedOperations) || !strings.Contains(err.Error(), "op-mail") {
		t.Fatalf("seal err = %v, want unresolved op-mail", err)
	}
	if a := authority(t, local, pid); a != "active" {
		t.Fatalf("refused seal left authority %s", a)
	}
	st := must(local.state.PersonaState(ctx, pid))
	if st.Lease == nil || st.Lease.Generation != gen || st.Lease.HolderID != "local-core" {
		t.Fatalf("refused seal changed the lease: %+v", st.Lease)
	}
	if _, err := local.svc.Status(ctx, "export", "move-0004"); !errors.Is(err, ErrTransferNotFound) {
		t.Fatalf("refused seal recorded a transfer: %v", err)
	}
}

// A job is runner-owned execution bound to the placement that queued it, so
// job rows are never carried. The seal refuses while one is queued, running
// or cancel-requested — finishing it detached on a sealed source would drop
// its result into a dead inbox, and carrying the claim could start the same
// work twice. A refused seal leaves the placement live: the runner still
// finishes the job, and its terminal notification crosses the boundary as an
// ordinary input. On the sealed source a fresh submit is refused while a
// replay of the accepted job still answers, and no claim can start work.
func TestJobsStayWithThePlacementThatRunsThem(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	cloudID := placementID(t, cloud)

	jobReq := map[string]any{"command": []any{"echo", "hi"}}
	if _, created, err := local.state.SubmitJob(ctx, pid, "j-1", "subprocess", jobReq, "api"); err != nil || !created {
		t.Fatalf("submit j-1: created=%v err=%v", created, err)
	}
	if _, err := local.svc.Seal(ctx, pid, "move-jobs", cloudID); !errors.Is(err, ErrUnresolvedOperations) ||
		!strings.Contains(err.Error(), "j-1") {
		t.Fatalf("seal with a queued job: %v, want unresolved j-1", err)
	}
	if a := authority(t, local, pid); a != "active" {
		t.Fatalf("refused seal left authority %s", a)
	}

	claimed, _, err := local.state.ClaimJobs(ctx, pid, "runner-1", []string{"subprocess"}, time.Minute, 4)
	if err != nil || len(claimed) != 1 || claimed[0].JobID != "j-1" {
		t.Fatalf("claim after refused seal: claimed=%+v err=%v", claimed, err)
	}
	if _, err := local.state.CompleteJob(ctx, pid, "j-1", "runner-1", "done",
		map[string]any{"exit_code": 0.0}, ""); err != nil {
		t.Fatalf("complete j-1: %v", err)
	}

	must(local.svc.Seal(ctx, pid, "move-jobs", cloudID))
	if _, _, err := local.state.SubmitJob(ctx, pid, "j-2", "subprocess", jobReq, "api"); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("submit after seal: %v, want persona inactive", err)
	}
	if j, created, err := local.state.SubmitJob(ctx, pid, "j-1", "subprocess", jobReq, "api"); err != nil || created || j.Status != "done" {
		t.Fatalf("replay j-1 after seal: created=%v status=%s err=%v", created, j.Status, err)
	}
	if claimed, _, err := local.state.ClaimJobs(ctx, pid, "runner-1", []string{"subprocess"}, time.Minute, 4); err != nil || len(claimed) != 0 {
		t.Fatalf("claim on the sealed persona: claimed=%+v err=%v", claimed, err)
	}

	bundle, _ := exportBytes(t, local, pid, "move-jobs")
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil); err != nil {
		t.Fatalf("import: %v", err)
	}
	act := must(cloud.svc.Activate(ctx, pid, "move-jobs"))
	must(local.svc.Complete(ctx, pid, "move-jobs", act.ActivateProof))

	var jobs, notes int
	if err := cloud.pool.QueryRow(ctx, `SELECT count(*) FROM core_jobs WHERE persona_id = $1`, pid).Scan(&jobs); err != nil || jobs != 0 {
		t.Fatalf("destination carries %d job rows (err=%v), want none", jobs, err)
	}
	if err := cloud.pool.QueryRow(ctx, `SELECT count(*) FROM core_inputs WHERE persona_id = $1 AND input_id = 'job:j-1'`, pid).
		Scan(&notes); err != nil || notes != 1 {
		t.Fatalf("destination job notification = %d (err=%v), want the one terminal input", notes, err)
	}
}

// A submit that loses the race with the seal is refused, never queued on the
// sealed source; one that wins is inside the cut, where the seal's in-flight
// check refuses the move. The share-locked persona row makes the two
// transactions serialize — no job can slip between the check and the commit.
func TestJobSubmitRacingTheSealLandsOnOneSide(t *testing.T) {
	ctx := context.Background()
	for i := 0; i < 24; i++ {
		local := newPlacement(t)
		pid := newID(t)
		liveSecretary(t, local, pid)
		jobReq := map[string]any{"command": []any{"echo", "hi"}}
		jobID := fmt.Sprintf("j-race-%d", i)
		destination := newID(t)

		submitErr := make(chan error, 1)
		go func() {
			_, _, err := local.state.SubmitJob(ctx, pid, jobID, "subprocess", jobReq, "api")
			submitErr <- err
		}()
		sealErr := make(chan error, 1)
		go func() {
			_, err := local.svc.Seal(ctx, pid, "move-race", destination)
			sealErr <- err
		}()
		sErr, jErr := <-sealErr, <-submitErr

		var queued bool
		if err := local.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM core_jobs WHERE persona_id = $1 AND job_id = $2)`,
			pid, jobID).Scan(&queued); err != nil {
			t.Fatal(err)
		}
		switch {
		case sErr == nil && jErr == nil && queued:
			t.Fatalf("race %d: seal committed yet the job it checked for was queued", i)
		case sErr == nil:
			if !errors.Is(jErr, agentstate.ErrPersonaInactive) {
				t.Fatalf("race %d: submit after seal committed: %v, want persona inactive", i, jErr)
			}
			if queued {
				t.Fatalf("race %d: refused submit left a job row", i)
			}
		case errors.Is(sErr, ErrUnresolvedOperations):
			if jErr != nil || !queued {
				t.Fatalf("race %d: seal refused for the job but submit err=%v queued=%v", i, jErr, queued)
			}
		default:
			t.Fatalf("race %d: unexpected seal=%v submit=%v queued=%v", i, sErr, jErr, queued)
		}
	}
}

// Stopping a transfer returns the one secretary to where it was: the staged
// copy is retired, the source continues under a newer generation once it has
// the destination's retire proof, and a later transfer of the continued life
// stages cleanly. A second copy of a persona that is already present is
// refused.
func TestAbortAndRetireKeepOneSecretary(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	cloudID := must(cloud.svc.PlacementID(ctx))
	sealed := must(local.svc.Seal(ctx, pid, "move-0005", cloudID))
	first, _ := exportBytes(t, local, pid, "move-0005")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(first), nil)))

	// Abort without evidence stays sealed; the staged copy alone is enough
	// to make an unproven abort unsafe.
	if _, err := local.svc.Abort(ctx, pid, "move-0005", ""); !errors.Is(err, ErrMissingProof) {
		t.Fatalf("abort without a retire proof: %v", err)
	}
	if _, err := local.svc.Abort(ctx, pid, "move-0005", strings.Repeat("b", 64)); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("abort with a foreign proof: %v", err)
	}
	if a := authority(t, local, pid); a != "sealed" {
		t.Fatalf("unproven abort changed authority to %s", a)
	}

	retired := retireDest(t, cloud, pid, "move-0005", "")
	if retired.Status != "retired" || retired.RetireProof == "" {
		t.Fatalf("retire produced %+v", retired)
	}
	if again := retireDest(t, cloud, pid, "move-0005", ""); again.RetireProof != retired.RetireProof {
		t.Fatal("retire replay produced a different proof")
	}
	if st := must(cloud.svc.Status(ctx, "import", "move-0005")); st.RetireProof != retired.RetireProof {
		t.Fatal("status does not return the retire proof after a lost response")
	}
	if _, err := cloud.state.PersonaState(ctx, pid); !errors.Is(err, agentstate.ErrPersonaNotFound) {
		t.Fatalf("retired persona still present: %v", err)
	}
	// Cancellation is durable: the retired transfer can never be staged or
	// activated here again, and a new transfer of the same persona can.
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(first), nil); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("re-import after retire: %v", err)
	}
	if _, err := cloud.svc.Activate(ctx, pid, "move-0005"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("activate after retire: %v", err)
	}

	must(local.svc.Abort(ctx, pid, "move-0005", retired.RetireProof))
	must(local.svc.Abort(ctx, pid, "move-0005", retired.RetireProof))
	if _, err := local.svc.Complete(ctx, pid, "move-0005", retired.RetireProof); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("complete after abort: %v", err)
	}

	lease := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute))
	if lease.Generation <= sealed.Cut.GenerationHighWater {
		t.Fatalf("source resumed at generation %d, not above the aborted epoch %d", lease.Generation, sealed.Cut.GenerationHighWater)
	}
	must(local.state.Recover(ctx, pid, lease.Generation))
	load := must(local.state.LoadTurn(ctx, pid, lease.Generation, "turn-2-local", 50))
	if load.Input == nil || load.Input.InputID != "in-2" || load.Plan == nil {
		t.Fatalf("source did not continue its interrupted turn: %+v", load)
	}
	must(local.state.CommitTurn(ctx, pid, "turn-2-local", lease.Generation, agentstate.CommitRequest{Outcome: "complete", Output: map[string]any{}}))
	if err := local.state.ReleaseWriter(ctx, pid, "local-core", lease.Generation); err != nil {
		t.Fatal(err)
	}

	must(local.svc.Seal(ctx, pid, "move-0006", cloudID))
	second, _ := exportBytes(t, local, pid, "move-0006")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(second), nil)))
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(first), nil); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("importing the retired cut beside the staged one: %v, want conflict", err)
	}
	// A different transfer's bundle for a persona already present is refused.
	stranger := bytes.Replace(second, []byte(`"transfer_id":"move-0006"`), []byte(`"transfer_id":"move-0006b"`), 1)
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(stranger), nil); !errors.Is(err, ErrPersonaExists) {
		t.Fatalf("importing another transfer for a present persona: %v, want ErrPersonaExists", err)
	}
	if _, err := local.svc.Seal(ctx, pid, "move-0005", cloudID); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("reusing an aborted transfer id: %v", err)
	}
}

// Inputs arriving while the seal commits are either inside the exported cut
// or explicitly refused — never acknowledged and then left behind.
func TestInputsRacingTheSealAreCarriedOrRefused(t *testing.T) {
	ctx := context.Background()
	local := newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "")))

	var mu sync.Mutex
	accepted, refused := map[string]bool{}, map[string]bool{}
	var wg sync.WaitGroup

	// The seal is scheduled deliberately, not by a wall-clock guess: it
	// begins once a counted slice of inputs has been accepted, while the
	// rest of the stream is still racing it, and each worker's final
	// submission is held until the seal returns. The cut is therefore
	// guaranteed to straddle the stream — at least startSealAfter accepted,
	// at least one refused per worker — with everything in between
	// genuinely concurrent with the sealing transaction.
	const startSealAfter = 30
	var nAccepted atomic.Int64
	sealAt := make(chan struct{})
	sealed := make(chan struct{})
	destination := newID(t)
	var sealErr error
	go func() {
		<-sealAt
		_, sealErr = local.svc.Seal(ctx, pid, "move-0007", destination)
		close(sealed)
	}()

	start := make(chan struct{})
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < 200; i++ {
				if i == 199 {
					<-sealed
				}
				id := fmt.Sprintf("race-%d-%03d", w, i)
				_, created, err := local.state.SubmitInput(ctx, &agentstate.Input{PersonaID: pid, InputID: id, Kind: "message", Payload: map[string]any{"i": i}})
				mu.Lock()
				switch {
				case err == nil && created:
					accepted[id] = true
					if nAccepted.Add(1) == startSealAfter {
						close(sealAt)
					}
				case errors.Is(err, agentstate.ErrPersonaInactive):
					refused[id] = true
				default:
					t.Errorf("submit %s: created=%v err=%v", id, created, err)
				}
				mu.Unlock()
			}
		}(w)
	}
	close(start)
	wg.Wait()
	if sealErr != nil {
		t.Fatalf("seal: %v", sealErr)
	}
	bundle, _ := exportBytes(t, local, pid, "move-0007")

	exported := map[string]bool{}
	sc := bufio.NewScanner(bytes.NewReader(bundle))
	for sc.Scan() {
		var row rowRecord
		if json.Unmarshal(sc.Bytes(), &row) != nil || row.Table != "core_inputs" {
			continue
		}
		var data struct {
			InputID string `json:"input_id"`
		}
		_ = json.Unmarshal(row.Data, &data)
		exported[data.InputID] = true
	}
	t.Logf("accepted=%d refused=%d exported=%d", len(accepted), len(refused), len(exported))
	if len(accepted) == 0 || len(refused) == 0 {
		t.Fatalf("race did not straddle the seal: accepted=%d refused=%d", len(accepted), len(refused))
	}
	for id := range accepted {
		if !exported[id] {
			t.Fatalf("accepted input %s is missing from the cut", id)
		}
	}
	for id := range refused {
		if exported[id] {
			t.Fatalf("refused input %s is inside the cut", id)
		}
	}
	if len(exported) != len(accepted) {
		t.Fatalf("cut has %d inputs, %d were accepted", len(exported), len(accepted))
	}
}

// Every table that belongs to a persona must be carried by core.v1 or be
// declared placement-local, and carried tables must match the contract's
// columns. A module that adds persona state without a portability decision
// fails here instead of being silently left behind by a transfer.
func TestEveryPersonaTableHasAPortabilityDecision(t *testing.T) {
	ctx := context.Background()
	p := newPlacement(t)
	referencing, err := strings_(ctx, p.pool, `
		SELECT DISTINCT tc.table_name::text FROM information_schema.table_constraints tc
		JOIN information_schema.constraint_column_usage ccu
		  ON ccu.constraint_name = tc.constraint_name AND ccu.constraint_schema = tc.constraint_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = current_schema()
		  AND ccu.table_name = 'core_personas' AND tc.table_name <> 'core_personas'`)
	if err != nil {
		t.Fatal(err)
	}
	carried := map[string]bool{}
	for _, tb := range coreTables {
		carried[tb.name] = true
	}
	for _, name := range referencing {
		if !carried[name] && placementLocalTables[name] == "" {
			t.Errorf("table %s references core_personas but has no portability decision", name)
		}
	}
	if len(referencing) != len(coreTables)+len(placementLocalTables) {
		t.Errorf("referencing tables %v; contract covers %d carried + %d local", referencing, len(coreTables), len(placementLocalTables))
	}
	if err := checkDestinationSchema(ctx, p.pool); err != nil {
		t.Fatalf("schema drifted from %s: %v", CoreContract, err)
	}
}

func TestTransferRoutesRequireTheServiceSecret(t *testing.T) {
	ctx := context.Background()
	local := newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	const secret = "portable-test-admin-secret"
	mux := http.NewServeMux()
	NewServer(local.pool, secret).RegisterRoutes(mux)
	personaToken := agentstate.NewServer(local.pool, secret).PersonaToken(pid)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	call := func(method, path, token string, body any) (int, string) {
		var rdr io.Reader
		if body != nil {
			raw, _ := json.Marshal(body)
			rdr = bytes.NewReader(raw)
		}
		req, _ := http.NewRequestWithContext(ctx, method, srv.URL+path, rdr)
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		_, _ = io.Copy(io.Discard, res.Body)
		return res.StatusCode, res.Header.Get("Content-Type")
	}
	base := "/internal/core/personas/" + pid + "/transfers/move-0008"
	if code, _ := call("GET", "/internal/core/placement", personaToken, nil); code != http.StatusUnauthorized {
		t.Fatalf("persona token read the placement id: %d", code)
	}
	if code, _ := call("POST", base+"/seal", personaToken, map[string]string{"destination_id": newID(t)}); code != http.StatusUnauthorized {
		t.Fatalf("persona token sealed its persona: %d", code)
	}
	if a := authority(t, local, pid); a != "active" {
		t.Fatalf("authority %s after refused seal", a)
	}
	if code, _ := call("GET", base+"/bundle", secret, nil); code != http.StatusConflict {
		t.Fatalf("export before seal: %d", code)
	}
	if code, _ := call("POST", base+"/seal", secret, nil); code != http.StatusBadRequest {
		t.Fatalf("seal without a destination: %d", code)
	}
	if code, _ := call("POST", base+"/seal", secret, map[string]string{"destination_id": newID(t)}); code != http.StatusOK {
		t.Fatalf("seal: %d", code)
	}
	if code, ct := call("GET", base+"/bundle", secret, nil); code != http.StatusOK || ct != "application/x-ndjson" {
		t.Fatalf("bundle: %d %s", code, ct)
	}
	if code, _ := call("GET", "/internal/core/transfers/export/move-0008", secret, nil); code != http.StatusOK {
		t.Fatalf("status: %d", code)
	}
}

// F1: an activate whose response was lost must not be able to fork the
// secretary. The source refuses to abort without the destination's retire
// proof; a destination that activated can never produce one. The correct
// recovery — read the destination's status, take the activate proof,
// complete — leaves exactly one placement able to run the secretary.
func TestLostActivateResponseKeepsOneWriter(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-0010", must(cloud.svc.PlacementID(ctx))))
	bundle, _ := exportBytes(t, local, pid, "move-0010")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil)))

	// Activate commits; the response is lost to the caller.
	must(cloud.svc.Activate(ctx, pid, "move-0010"))

	// The mistaken coordinator retries by aborting the source. No proof
	// exists: abort is refused and the source stays sealed, not dual.
	if _, err := local.svc.Abort(ctx, pid, "move-0010", ""); !errors.Is(err, ErrMissingProof) {
		t.Fatalf("abort after lost activate: %v, want ErrMissingProof", err)
	}
	if _, err := local.svc.Abort(ctx, pid, "move-0010", strings.Repeat("c", 64)); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("abort with a fabricated proof: %v", err)
	}
	if a := authority(t, local, pid); a != "sealed" {
		t.Fatalf("refused abort changed authority to %s", a)
	}
	// The destination cannot be talked out of it either.
	if _, err := cloud.svc.Retire(ctx, pid, "move-0010", placementID(t, cloud), ""); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("retire after activate: %v", err)
	}

	// The recoverable path: status reveals the committed activation.
	st := must(cloud.svc.Status(ctx, "import", "move-0010"))
	if st.Status != "activated" || st.ActivateProof == "" {
		t.Fatalf("status after lost activate: %+v", st)
	}
	must(local.svc.Complete(ctx, pid, "move-0010", st.ActivateProof))

	// Exactly one placement can run the secretary.
	if _, err := local.state.AcquireWriter(ctx, pid, "local-core", time.Minute); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("source acquired a writer after completion: %v", err)
	}
	if _, err := cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute); err != nil {
		t.Fatalf("destination writer: %v", err)
	}
}

// The import's lease epoch floor must be dead on arrival under any clock:
// the generation travels so the first destination writer acquires
// floor+1, but the expiry itself is a fixed past instant — a now()
// expiry can look live to a later transaction after the host clock steps
// backward (observed on WSL2: journald "Time jumped backwards"), which
// once surfaced here as ErrWriterHeld on the first destination acquire.
func TestImportLeaseFloorIsDeadOnArrival(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-0010b", must(cloud.svc.PlacementID(ctx))))
	bundle, _ := exportBytes(t, local, pid, "move-0010b")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil)))

	var floorGen int64
	var holder string
	var expires time.Time
	if err := cloud.pool.QueryRow(ctx,
		`SELECT generation, holder_id, expires_at FROM core_writer_leases WHERE persona_id = $1`,
		pid).Scan(&floorGen, &holder, &expires); err != nil {
		t.Fatalf("read floor lease: %v", err)
	}
	if holder != "transfer:move-0010b" {
		t.Fatalf("floor lease holder %q", holder)
	}
	if expires.After(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("floor lease expiry is not a fixed past instant: %s", expires)
	}
	must(cloud.svc.Activate(ctx, pid, "move-0010b"))
	lease := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute))
	if lease.Generation != floorGen+1 {
		t.Fatalf("first destination writer generation %d, want floor %d + 1", lease.Generation, floorGen)
	}
}

// F3: a bundle is addressed to one placement. Staging it anywhere else is
// refused, so a routine "import to A timed out, try B" retry can never leave
// two activatable copies.
func TestBundleIsBoundToOneDestination(t *testing.T) {
	ctx := context.Background()
	local, cloudA, cloudB := newPlacement(t), newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	destA := must(cloudA.svc.PlacementID(ctx))
	destB := must(cloudB.svc.PlacementID(ctx))
	must(local.svc.Seal(ctx, pid, "move-0011", destA))
	bundle, _ := exportBytes(t, local, pid, "move-0011")
	if got := bundleHeader(t, bundle).DestinationID; got != destA {
		t.Fatalf("bundle addressed to %s, want %s", got, destA)
	}

	// The ordinary retry onto the wrong placement is refused.
	if _, _, err := cloudB.svc.Import(ctx, bytes.NewReader(bundle), nil); !errors.Is(err, ErrBadBundle) ||
		!strings.Contains(err.Error(), "addressed to placement") {
		t.Fatalf("import on the wrong placement: %v", err)
	}
	must(drop(cloudA.svc.Import(ctx, bytes.NewReader(bundle), nil)))
	act := must(cloudA.svc.Activate(ctx, pid, "move-0011"))
	if _, err := cloudB.svc.Activate(ctx, pid, "move-0011"); !errors.Is(err, ErrTransferNotFound) &&
		!errors.Is(err, ErrPersonaNotFound) && !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("second destination activated: %v", err)
	}
	must(local.svc.Complete(ctx, pid, "move-0011", act.ActivateProof))
	if _, err := cloudB.state.AcquireWriter(ctx, pid, "cloud-b", time.Minute); err == nil {
		t.Fatal("the unaddressed placement acquired a writer")
	}
	if _, err := cloudA.state.AcquireWriter(ctx, pid, "cloud-a", time.Minute); err != nil {
		t.Fatalf("addressed placement writer: %v", err)
	}
	_ = destB
}

// F4: complete cannot be satisfied by the source's own records — it needs the
// proof the destination produced when it activated. Without a destination the
// source stays sealed, and the retire→abort path still recovers it.
func TestCompleteRequiresDestinationActivation(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	localGen := liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-0012", must(cloud.svc.PlacementID(ctx))))
	bundle, exported := exportBytes(t, local, pid, "move-0012")

	// No import anywhere. The old contract accepted the export digest here.
	if _, err := local.svc.Complete(ctx, pid, "move-0012", exported.ContentSHA256); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("complete with the source's own digest: %v", err)
	}
	if _, err := local.svc.Complete(ctx, pid, "move-0012", ""); !errors.Is(err, ErrMissingProof) {
		t.Fatalf("complete with no proof: %v", err)
	}
	if a := authority(t, local, pid); a != "sealed" {
		t.Fatalf("refused complete changed authority to %s — a stranded secretary", a)
	}

	// The bundle never arrived at the destination either. Retire records a
	// tombstone keyed by the header's transfer key, and abort unseals.
	key := bundleHeader(t, bundle).TransferKey
	if _, err := cloud.svc.Retire(ctx, pid, "move-0012", placementID(t, cloud), ""); !errors.Is(err, ErrMissingProof) {
		t.Fatalf("tombstone retire without the key: %v", err)
	}
	tomb := retireDest(t, cloud, pid, "move-0012", key)
	if tomb.Status != "retired" || tomb.RetireProof == "" {
		t.Fatalf("tombstone retire: %+v", tomb)
	}
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("a late import beats the tombstone: %v", err)
	}
	must(local.svc.Abort(ctx, pid, "move-0012", tomb.RetireProof))
	lease := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute))
	if lease.Generation <= localGen {
		t.Fatalf("source resumed at generation %d", lease.Generation)
	}
}

// A retire aimed at the transfer's own source must not produce a valid
// retire proof — otherwise a misconfigured call to the wrong service would
// release the seal while a staged copy lives elsewhere.
func TestRetireRefusesTheSourcePlacement(t *testing.T) {
	ctx := context.Background()
	local := newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-0013", newID(t)))
	bundle, _ := exportBytes(t, local, pid, "move-0013")
	key := bundleHeader(t, bundle).TransferKey
	if _, err := local.svc.Retire(ctx, pid, "move-0013", placementID(t, local), key); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("retire on the source: %v", err)
	}
	if _, err := local.svc.Status(ctx, "import", "move-0013"); !errors.Is(err, ErrTransferNotFound) {
		t.Fatalf("source retire left an import ledger row: %v", err)
	}
}

// Retire and activate on the same staged transfer race to one committed
// outcome — never both.
func TestRetireAndActivateCommitExactlyOnce(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-0014", must(cloud.svc.PlacementID(ctx))))
	bundle, _ := exportBytes(t, local, pid, "move-0014")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil)))

	results := make(chan error, 2)
	go func() { _, err := cloud.svc.Activate(ctx, pid, "move-0014"); results <- err }()
	go func() { _, err := cloud.svc.Retire(ctx, pid, "move-0014", placementID(t, cloud), ""); results <- err }()
	err1, err2 := <-results, <-results
	if (err1 == nil) == (err2 == nil) {
		t.Fatalf("retire and activate both committed or both failed: %v / %v", err1, err2)
	}
	// The source's next step is consistent with whichever won: a retire
	// proof completes abort, an activate proof completes the transfer.
	st := must(cloud.svc.Status(ctx, "import", "move-0014"))
	switch st.Status {
	case "retired":
		must(local.svc.Abort(ctx, pid, "move-0014", st.RetireProof))
		if _, err := cloud.svc.Activate(ctx, pid, "move-0014"); !errors.Is(err, ErrTransferConflict) {
			t.Fatalf("activated after retire: %v", err)
		}
	case "activated":
		must(local.svc.Complete(ctx, pid, "move-0014", st.ActivateProof))
		if _, err := local.svc.Abort(ctx, pid, "move-0014", ""); !errors.Is(err, ErrTransferConflict) {
			t.Fatalf("aborted after completion: %v", err)
		}
	default:
		t.Fatalf("transfer status %s", st.Status)
	}
}

// A slow import and a cancellation tombstone race to one committed outcome:
// either the import lands first and retire deletes the staged copy, or the
// tombstone lands first and the import is refused. Never a staged copy under
// a retired transfer.
func TestImportAndRetireRaceToOneOutcome(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-0015", must(cloud.svc.PlacementID(ctx))))
	bundle, _ := exportBytes(t, local, pid, "move-0015")
	key := bundleHeader(t, bundle).TransferKey

	type outcome struct {
		imported bool
		retired  bool
		err      error
	}
	results := make(chan outcome, 2)
	go func() {
		_, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil)
		results <- outcome{imported: created, err: err}
	}()
	go func() {
		_, err := cloud.svc.Retire(ctx, pid, "move-0015", placementID(t, cloud), key)
		results <- outcome{retired: err == nil, err: err}
	}()
	a, b := <-results, <-results
	st, statusErr := cloud.svc.Status(ctx, "import", "move-0015")
	personaErr := func() error { _, e := cloud.state.PersonaState(ctx, pid); return e }()
	t.Logf("outcomes: %+v / %+v; final status %v (%v), persona err %v", a.err, b.err, st.Status, statusErr, personaErr)
	if statusErr != nil || st.Status != "retired" {
		t.Fatalf("final transfer status %v, want retired", st.Status)
	}
	if !errors.Is(personaErr, agentstate.ErrPersonaNotFound) {
		t.Fatalf("a staged persona survived its retirement: %v", personaErr)
	}
	if _, err := cloud.svc.Activate(ctx, pid, "move-0015"); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("activated after retire: %v", err)
	}
}

// There is no force path on abort: a destination that cannot answer leaves
// the source sealed. The HTTP route rejects {force:true} explicitly so the
// flag can never silently release authority.
func TestAbortHasNoForcePath(t *testing.T) {
	ctx := context.Background()
	local := newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-0016", newID(t)))

	const secret = "portable-test-admin-secret"
	mux := http.NewServeMux()
	NewServer(local.pool, secret).RegisterRoutes(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	req := func(body string) int {
		r, _ := http.NewRequestWithContext(ctx, "POST",
			srv.URL+"/internal/core/personas/"+pid+"/transfers/move-0016/abort",
			strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+secret)
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		_, _ = io.Copy(io.Discard, res.Body)
		return res.StatusCode
	}
	if code := req(`{"force":true}`); code != http.StatusBadRequest {
		t.Fatalf("force abort: %d, want 400", code)
	}
	if code := req(`{}`); code != http.StatusBadRequest {
		t.Fatalf("proofless abort: %d, want 400", code)
	}
	if a := authority(t, local, pid); a != "sealed" {
		t.Fatalf("refused aborts changed authority to %s", a)
	}
}

// F5: import errors and replay parameters name what actually went wrong.
func TestImportErrorContract(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	must(local.svc.Seal(ctx, pid, "move-0017", must(cloud.svc.PlacementID(ctx))))
	bundle, _ := exportBytes(t, local, pid, "move-0017")

	// A structurally valid bundle carrying a duplicate key is a
	// deterministic 422, not a "repeat the call" 409.
	dup := mutateLines(bundle, func(lines []string) []string {
		for i, line := range lines {
			if strings.Contains(line, `"table":"core_inputs"`) {
				return append(lines[:i+1], append([]string{line}, lines[i+1:]...)...)
			}
		}
		return lines
	})
	dup = rehashBundle(t, dup)
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(dup), nil); !errors.Is(err, ErrBadBundle) ||
		!strings.Contains(err.Error(), "duplicates a key") {
		t.Fatalf("duplicate-key bundle: %v", err)
	}

	// A human_id that does not exist names the parameter, not the bundle.
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), ptr(newID(t))); !errors.Is(err, ErrBadRequest) ||
		!strings.Contains(err.Error(), "human_id") {
		t.Fatalf("missing human: %v", err)
	}

	humanID := newID(t)
	if _, err := cloud.pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, humanID); err != nil {
		t.Fatal(err)
	}
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID)))
	// Replays must carry the same human_id; a different one is a conflict,
	// never a silent rebind.
	other := newID(t)
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &other); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("replay with a different human_id: %v", err)
	}
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID); err != nil || created {
		t.Fatalf("faithful replay: created=%v err=%v", created, err)
	}
}

// R7: a retire dispatched to a service that never saw the bundle cannot end
// the source's authority. The request itself must name this placement, and
// the proof any placement mints names its own id — the source verifies it
// against the destination it sealed to.
func TestRetireProofBindsTheDestination(t *testing.T) {
	ctx := context.Background()
	local, cloud, third := newPlacement(t), newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	cloudID := placementID(t, cloud)
	thirdID := placementID(t, third)
	must(local.svc.Seal(ctx, pid, "move-0020", cloudID))
	bundle, _ := exportBytes(t, local, pid, "move-0020")
	key := bundleHeader(t, bundle).TransferKey
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil)))

	// The request itself must name this placement: a retire addressed to
	// cloud is refused by third before anything is recorded.
	if _, err := third.svc.Retire(ctx, pid, "move-0020", cloudID, key); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("retire addressed elsewhere: %v", err)
	}
	if _, err := third.svc.Status(ctx, "import", "move-0020"); !errors.Is(err, ErrTransferNotFound) {
		t.Fatalf("the refused retire still wrote a tombstone: %v", err)
	}

	// Even when the mistaken call names the wrong service's own id, the
	// proof minted there names that placement — the source rejects it, and
	// the staged copy on the real destination is not freed to activate beside
	// a resumed source.
	bogus := must(third.svc.Retire(ctx, pid, "move-0020", thirdID, key))
	if bogus.Status != "retired" || bogus.RetireProof == "" {
		t.Fatalf("third's tombstone: %+v", bogus)
	}
	if _, err := local.svc.Abort(ctx, pid, "move-0020", bogus.RetireProof); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("abort on a proof minted by the wrong placement: %v", err)
	}
	if a := authority(t, local, pid); a != "sealed" {
		t.Fatalf("a foreign retire proof changed authority to %s", a)
	}

	// The honest resolution: retire on the addressed destination deletes the
	// staged copy, and its proof — naming cloud — unseals the source.
	retired := retireDest(t, cloud, pid, "move-0020", "")
	must(local.svc.Abort(ctx, pid, "move-0020", retired.RetireProof))
	if _, err := local.state.AcquireWriter(ctx, pid, "local-core", time.Minute); err != nil {
		t.Fatalf("source did not resume: %v", err)
	}
	if _, err := cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute); err == nil {
		t.Fatal("the retired destination acquired a writer — two writers")
	}
}

// R8: a tombstone written from mistaken parameters — a wrong transfer_key —
// stays correctable while nothing was imported. Correction rewrites the
// record and re-mints the proof; it never lifts the foreclosure.
func TestTombstoneCorrectionRecoversAWrongKey(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	cloudID := placementID(t, cloud)
	must(local.svc.Seal(ctx, pid, "move-0021", cloudID))
	bundle, _ := exportBytes(t, local, pid, "move-0021")
	key := bundleHeader(t, bundle).TransferKey

	// The retire reached the right service but carried the wrong key: its
	// proof can never satisfy the source, and replaying the same wrong call
	// keeps returning it.
	wrong := must(cloud.svc.Retire(ctx, pid, "move-0021", cloudID, strings.Repeat("d", 64)))
	if _, err := local.svc.Abort(ctx, pid, "move-0021", wrong.RetireProof); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("abort on a proof minted under the wrong key: %v", err)
	}
	if again := must(cloud.svc.Retire(ctx, pid, "move-0021", cloudID, strings.Repeat("d", 64))); again.RetireProof != wrong.RetireProof {
		t.Fatal("repeating the bad key minted a different proof")
	}
	// Correcting without the key is refused; with the real key the tombstone
	// is rewritten in place and its proof re-minted.
	if _, err := cloud.svc.Retire(ctx, pid, "move-0021", cloudID, ""); !errors.Is(err, ErrMissingProof) {
		t.Fatalf("correction without the key: %v", err)
	}
	fixed := must(cloud.svc.Retire(ctx, pid, "move-0021", cloudID, key))
	if fixed.RetireProof == wrong.RetireProof {
		t.Fatal("correcting the key did not re-mint the proof")
	}
	must(local.svc.Abort(ctx, pid, "move-0021", fixed.RetireProof))
	// Foreclosure is durable through correction.
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("import after tombstone correction: %v", err)
	}
	if _, err := local.state.AcquireWriter(ctx, pid, "local-core", time.Minute); err != nil {
		t.Fatalf("source did not resume: %v", err)
	}
}

// The other half of R8: a tombstone naming the wrong persona is corrected the
// same way — and while it stands, the real bundle is refused on the
// persona mismatch rather than staged beside it.
func TestTombstoneCorrectionRecoversAWrongPersona(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	cloudID := placementID(t, cloud)
	must(local.svc.Seal(ctx, pid, "move-0022", cloudID))
	bundle, _ := exportBytes(t, local, pid, "move-0022")
	key := bundleHeader(t, bundle).TransferKey

	mistaken := must(cloud.svc.Retire(ctx, newID(t), "move-0022", cloudID, key))
	if mistaken.PersonaID == pid {
		t.Fatal("the tombstone recorded the requested persona")
	}
	if _, err := local.svc.Abort(ctx, pid, "move-0022", mistaken.RetireProof); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("abort on a proof naming another persona: %v", err)
	}
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("the real bundle staged beside a wrong-persona tombstone: %v", err)
	}
	fixed := must(cloud.svc.Retire(ctx, pid, "move-0022", cloudID, key))
	if fixed.PersonaID != pid {
		t.Fatalf("correction kept persona %s", fixed.PersonaID)
	}
	must(local.svc.Abort(ctx, pid, "move-0022", fixed.RetireProof))
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("import after persona correction: %v", err)
	}
}

// An imported-then-retired transfer is never rewritten: its parameters were
// verified against the bundle at import, so a replay with a different key is
// a conflict, not a correction.
func TestImportedRetireIsNotCorrectable(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	cloudID := placementID(t, cloud)
	must(local.svc.Seal(ctx, pid, "move-0023", cloudID))
	bundle, _ := exportBytes(t, local, pid, "move-0023")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil)))
	retired := retireDest(t, cloud, pid, "move-0023", "")
	if _, err := cloud.svc.Retire(ctx, pid, "move-0023", cloudID, strings.Repeat("e", 64)); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("re-retire of an imported transfer with a different key: %v", err)
	}
	if _, err := cloud.svc.Retire(ctx, newID(t), "move-0023", cloudID, ""); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("re-retire of an imported transfer with a different persona: %v", err)
	}
	if again := retireDest(t, cloud, pid, "move-0023", ""); again.RetireProof != retired.RetireProof {
		t.Fatal("imported retire replay changed the proof")
	}
	must(local.svc.Abort(ctx, pid, "move-0023", retired.RetireProof))
}

func ptr[T any](v T) *T { return &v }

// rehashBundle recomputes a mutated bundle's trailer digest, producing a
// structurally valid stream whose contents are wrong.
func rehashBundle(t *testing.T, bundle []byte) []byte {
	t.Helper()
	lines := strings.SplitAfter(string(bundle), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	h := sha256.New()
	for _, line := range lines[:len(lines)-1] {
		h.Write([]byte(line))
	}
	var tr Trailer
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &tr); err != nil {
		t.Fatal(err)
	}
	tr.ContentSHA256 = hex.EncodeToString(h.Sum(nil))
	raw, err := json.Marshal(tr)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(strings.Join(append(lines[:len(lines)-1], string(raw)+"\n"), ""))
}
