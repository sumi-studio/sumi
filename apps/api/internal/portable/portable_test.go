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
	must(drop(p.state.SavePlan(ctx, personaID, "turn-1", gen, plan1)))
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
	must(drop(p.state.SavePlan(ctx, personaID, "turn-2", gen, plan2)))
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

	rec := must(local.svc.Seal(ctx, pid, "move-0001"))
	if rec.Cut.GenerationHighWater != localGen+1 {
		t.Fatalf("epoch = %d, want %d", rec.Cut.GenerationHighWater, localGen+1)
	}
	want := Continuity{JournalEvents: 3, Notes: 2, QueuedInputs: 1, ClaimedInputs: 1, RunningTurns: 1,
		UnfinishedPlans: 1, PendingSchedules: 1, UndeliveredOut: 1}
	if rec.Continuity != want {
		t.Fatalf("continuity = %+v, want %+v", rec.Continuity, want)
	}
	if again := must(local.svc.Seal(ctx, pid, "move-0001")); !again.SealedAt.Equal(rec.SealedAt) {
		t.Fatalf("seal replay produced a new cut: %v vs %v", again.SealedAt, rec.SealedAt)
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
		t.Fatalf("complete with a foreign digest: %v", err)
	}
	must(cloud.svc.Activate(ctx, pid, "move-0001"))
	must(cloud.svc.Activate(ctx, pid, "move-0001"))
	must(local.svc.Complete(ctx, pid, "move-0001", staged.ContentSHA256))
	must(local.svc.Complete(ctx, pid, "move-0001", staged.ContentSHA256))
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
	if load.Input == nil || load.Input.InputID != "in-2" || load.Turn.Attempt != 2 || load.Plan == nil || len(load.Plan.Plan.Calls) != 2 {
		t.Fatalf("resumed turn %+v input %+v plan %+v", load.Turn, load.Input, load.Plan)
	}
	op, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-2b", gen, "op-2b-a", "journal.note", 0, load.Plan.Plan.Calls[0].Request)
	if err != nil || fresh || op.OperationID != "op-2-a" {
		t.Fatalf("carried call 0: op=%+v fresh=%v err=%v", op, fresh, err)
	}
	if _, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-2b", gen, "op-2b-b", "journal.note", 1, load.Plan.Plan.Calls[1].Request); err != nil || !fresh {
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
	must(local.svc.Seal(ctx, pid, "move-0002"))
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
	must(local.svc.Seal(ctx, pid, "move-0003"))
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
	if _, err := local.svc.Seal(ctx, pid, "move-0004"); !errors.Is(err, ErrUnresolvedOperations) || !strings.Contains(err.Error(), "op-mail") {
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

// Stopping a transfer returns the one secretary to where it was: the staged
// copy is discarded, the source continues under a newer generation, and a
// later transfer of the continued life stages cleanly. A second copy of a
// persona that is already present is refused.
func TestAbortAndDiscardKeepOneSecretary(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)
	sealed := must(local.svc.Seal(ctx, pid, "move-0005"))
	first, _ := exportBytes(t, local, pid, "move-0005")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(first), nil)))

	must(cloud.svc.Discard(ctx, pid, "move-0005"))
	must(cloud.svc.Discard(ctx, pid, "move-0005"))
	if _, err := cloud.state.PersonaState(ctx, pid); !errors.Is(err, agentstate.ErrPersonaNotFound) {
		t.Fatalf("discarded persona still present: %v", err)
	}
	must(local.svc.Abort(ctx, pid, "move-0005"))
	must(local.svc.Abort(ctx, pid, "move-0005"))
	if _, err := local.svc.Complete(ctx, pid, "move-0005", strings.Repeat("a", 64)); !errors.Is(err, ErrTransferConflict) {
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

	must(local.svc.Seal(ctx, pid, "move-0006"))
	second, _ := exportBytes(t, local, pid, "move-0006")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(second), nil)))
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(first), nil); !errors.Is(err, ErrPersonaExists) {
		t.Fatalf("importing the older cut beside the staged one: %v, want ErrPersonaExists", err)
	}
	if _, err := local.svc.Seal(ctx, pid, "move-0005"); !errors.Is(err, ErrTransferConflict) {
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
	start := make(chan struct{})
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < 200; i++ {
				id := fmt.Sprintf("race-%d-%03d", w, i)
				_, created, err := local.state.SubmitInput(ctx, &agentstate.Input{PersonaID: pid, InputID: id, Kind: "message", Payload: map[string]any{"i": i}})
				mu.Lock()
				switch {
				case err == nil && created:
					accepted[id] = true
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
	time.Sleep(20 * time.Millisecond)
	must(local.svc.Seal(ctx, pid, "move-0007"))
	wg.Wait()
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

	call := func(method, path, token string) (int, string) {
		req, _ := http.NewRequestWithContext(ctx, method, srv.URL+path, nil)
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
	if code, _ := call("POST", base+"/seal", personaToken); code != http.StatusUnauthorized {
		t.Fatalf("persona token sealed its persona: %d", code)
	}
	if a := authority(t, local, pid); a != "active" {
		t.Fatalf("authority %s after refused seal", a)
	}
	if code, _ := call("GET", base+"/bundle", secret); code != http.StatusConflict {
		t.Fatalf("export before seal: %d", code)
	}
	if code, _ := call("POST", base+"/seal", secret); code != http.StatusOK {
		t.Fatalf("seal: %d", code)
	}
	if code, ct := call("GET", base+"/bundle", secret); code != http.StatusOK || ct != "application/x-ndjson" {
		t.Fatalf("bundle: %d %s", code, ct)
	}
	if code, _ := call("GET", "/internal/core/transfers/export/move-0008", secret); code != http.StatusOK {
		t.Fatalf("status: %d", code)
	}
}
