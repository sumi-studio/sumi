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
	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
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
	return agentstate.PlanCall{Tool: "journal.note", Route: "normal", Request: map[string]any{"text": text}}
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
	reminder := agentstate.PlanCall{Tool: "schedule.set", Route: "normal", Request: map[string]any{
		"schedule_id": "sch-tea",
		"wake_at":     time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano),
		"payload":     map[string]any{"text": "tea time"},
	}}
	plan1 := agentstate.Decision{Text: "noted", Calls: []agentstate.PlanCall{note("The user takes tea at 15:00"), reminder}}
	must(drop(p.state.SavePlan(ctx, personaID, "turn-1", gen, 0, plan1)))
	for i, c := range plan1.Calls {
		if _, _, _, err := p.state.ClaimOperation(ctx, personaID, "turn-1", gen, fmt.Sprintf("op-1-%d", i), c.Tool, i, c.Request); err != nil {
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
	if _, _, _, err := p.state.ClaimOperation(ctx, personaID, "turn-2", gen, "op-2-a", "journal.note", 0, plan2.Calls[0].Request); err != nil {
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
	// admission_seq is destination-allocated on import, so normalize it out
	// of each row's data before comparing a source's rows to a destination's.
	// The comparison decodes each row instead of matching the bundle's byte
	// shape, so a change in how PostgreSQL renders jsonb cannot silently
	// disable it — TestImportIntoPopulatedDestinationKeepsAdmissionOrder
	// compares across placements whose identity values genuinely differ and
	// fails if no carried value is normalized.
	var out bytes.Buffer
	for _, line := range bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte("\n")) {
		var row rowRecord
		if err := strictDecode(line, &row); err != nil {
			t.Fatalf("bundle line does not decode: %v", err)
		}
		if col, ok := identityCols[row.Table]; ok {
			row.Data = replaceField(t, row.Data, col, json.RawMessage(`0`))
		}
		enc, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		out.Write(enc)
		out.WriteByte('\n')
	}
	return out.Bytes()
}

// replaceField returns data with one field's value swapped, preserving the
// other fields byte-for-byte.
func replaceField(t *testing.T, data json.RawMessage, name string, v json.RawMessage) json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatalf("row data is not a JSON object: %v", err)
	}
	if _, ok := fields[name]; !ok {
		t.Fatalf("row data lacks field %s", name)
	}
	fields[name] = v
	enc, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

// rewriteBundleAdmissionSeqs returns a copy of bundle with each core_inputs
// row's carried admission_seq replaced by seqs[i] in bundle order and the
// trailer's digest recomputed over the new content, so the rewritten bundle
// still passes import verification. It fails the test if the bundle carries
// a different number of identity rows than len(seqs), if a rewritten row
// does not carry the intended value, or if the result is byte-identical —
// a change in the bundle's byte shape must never turn this into a no-op
// that imports the original small values.
func rewriteBundleAdmissionSeqs(t *testing.T, bundle []byte, seqs []int64) []byte {
	t.Helper()
	lines := bytes.Split(bundle, []byte("\n"))
	if len(lines[len(lines)-1]) != 0 {
		t.Fatal("bundle does not end with a newline")
	}
	lines = lines[:len(lines)-1]
	var trailer Trailer
	if err := strictDecode(lines[len(lines)-1], &trailer); err != nil || trailer.Record != "trailer" {
		t.Fatalf("last bundle line is not a trailer: %v", err)
	}
	h := sha256.New()
	var out bytes.Buffer
	n := 0
	for _, line := range lines[:len(lines)-1] {
		var kind struct {
			Record string `json:"record"`
		}
		if err := json.Unmarshal(line, &kind); err != nil {
			t.Fatalf("malformed bundle line: %v", err)
		}
		if kind.Record == "row" {
			var row rowRecord
			if err := strictDecode(line, &row); err != nil {
				t.Fatalf("row line: %v", err)
			}
			if col, ok := identityCols[row.Table]; ok {
				if n >= len(seqs) {
					t.Fatalf("bundle carries more %s.%s rows than the %d replacement values",
						row.Table, col, len(seqs))
				}
				row.Data = replaceField(t, row.Data, col,
					json.RawMessage(fmt.Sprintf("%d", seqs[n])))
				var carried int64
				if err := unmarshalField(row.Data, col, &carried); err != nil || carried != seqs[n] {
					t.Fatalf("rewritten row carries %d, want %d: %v", carried, seqs[n], err)
				}
				var err error
				line, err = json.Marshal(row)
				if err != nil {
					t.Fatal(err)
				}
				n++
			}
		}
		out.Write(line)
		out.WriteByte('\n')
		h.Write(line)
		h.Write([]byte{'\n'})
	}
	if n != len(seqs) {
		t.Fatalf("rewrote %d identity values, want %d — the bundle's row shape changed", n, len(seqs))
	}
	trailer.ContentSHA256 = hex.EncodeToString(h.Sum(nil))
	tl, err := json.Marshal(trailer)
	if err != nil {
		t.Fatal(err)
	}
	out.Write(tl)
	out.WriteByte('\n')
	if bytes.Equal(out.Bytes(), bundle) {
		t.Fatal("rewritten bundle is byte-identical to the original — the rewrite did not apply")
	}
	return out.Bytes()
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
	// Each mid-turn journal.note first journaled its input (received_seq), so
	// the journal holds 5 events: in-1's input_received + note + commit, and
	// in-2's input_received + note (its turn never committed).
	want := Continuity{JournalEvents: 5, Notes: 2, QueuedInputs: 1, ClaimedInputs: 1, RunningTurns: 1,
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
	staged, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID, false)
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
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID, false); err != nil || created {
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
	op, _, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-2b", gen, "op-2b-a", "journal.note", 0, load.Plan.Plan[0].Calls[0].Request)
	if err != nil || fresh || op.OperationID != "op-2-a" {
		t.Fatalf("carried call 0: op=%+v fresh=%v err=%v", op, fresh, err)
	}
	if _, _, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-2b", gen, "op-2b-b", "journal.note", 1, load.Plan.Plan[0].Calls[1].Request); err != nil || !fresh {
		t.Fatalf("remaining call 1: fresh=%v err=%v", fresh, err)
	}
	// The resumed turn presents its input_received copy at commit, as the core
	// always does. in-2's copy was already journaled on the source — before the
	// crash, when its first note landed mid-turn — and received_seq carried
	// across the transfer, so neither the fresh note claim above nor this
	// commit may journal it a second time.
	must(cloud.state.CommitTurn(ctx, pid, "turn-2b", gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events: []agentstate.EventInput{{Kind: "input_received", Payload: map[string]any{
			"input_id": "in-2", "kind": "message", "text": "we are moving to the cloud",
			"actor_kind": "human", "source_surface": "test", "attempt": 2,
		}}},
		Output: map[string]any{"text": "moving"},
	}))

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
	var receivedSeq, secondNoteSeq int64
	received := 0
	for _, e := range must(cloud.state.Events(ctx, pid, 0, 1000)) {
		if e.Kind == "input_received" && e.Payload["input_id"] == "in-2" {
			received++
			receivedSeq = e.Seq
		}
		if e.Kind == "note" && e.Payload["text"] == "Second note after the move" {
			secondNoteSeq = e.Seq
		}
	}
	if received != 1 {
		t.Fatalf("in-2 input_received journaled %d times on the destination, want 1", received)
	}
	if secondNoteSeq <= receivedSeq {
		t.Fatalf("the resumed turn's note at seq %d does not follow its input at %d", secondNoteSeq, receivedSeq)
	}
	var carriedSeq int64
	if err := cloud.pool.QueryRow(ctx,
		`SELECT received_seq FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-2'`,
		pid).Scan(&carriedSeq); err != nil || carriedSeq != receivedSeq {
		t.Fatalf("carried received_seq %d (err %v) does not point at the input_received at %d", carriedSeq, err, receivedSeq)
	}
	after := must(cloud.state.Events(ctx, pid, rec.Cut.LatestEventSeq, 100))
	if len(after) == 0 || after[0].Seq != rec.Cut.LatestEventSeq+1 {
		t.Fatalf("journal does not continue after the cut: %+v", after)
	}
	if _, err := local.state.AcquireWriter(ctx, pid, "local-core", time.Minute); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("source acquire after completion: %v", err)
	}
}

// claimSeq claims the oldest claimable chunk and fails the test unless it is
// the expected one — the sequence is the assertion that sealing order and
// claim order agree.
func claimSeq(t *testing.T, p placement, personaID string, generation, wantSeq int64) {
	t.Helper()
	claimed := must(p.state.ClaimMemoryChunk(context.Background(), personaID, generation, 50))
	if claimed.Chunk == nil || claimed.Chunk.ChunkSeq != wantSeq {
		t.Fatalf("claimed chunk %+v, want chunk_seq %d", claimed.Chunk, wantSeq)
	}
}

// chunkRow reads one chunk's carried lifecycle fields.
func chunkRow(t *testing.T, p placement, personaID string, chunkSeq int64) agentstate.MemoryChunk {
	t.Helper()
	var c agentstate.MemoryChunk
	err := p.pool.QueryRow(context.Background(), `
		SELECT persona_id, chunk_seq, layer, sources, first_seq, last_seq, est_tokens,
			status, replacement, replacement_est_tokens, attempts, interruptions, last_error,
			claimed_generation, claimed_at, not_before, created_at, prepared_at, applied_at
		FROM core_memory_chunks WHERE persona_id = $1 AND chunk_seq = $2`,
		personaID, chunkSeq).Scan(&c.PersonaID, &c.ChunkSeq, &c.Layer, &c.Sources, &c.FirstSeq,
		&c.LastSeq, &c.EstTokens, &c.Status, &c.Replacement, &c.ReplacementEstTokens, &c.Attempts,
		&c.Interruptions, &c.LastError, &c.ClaimedGeneration, &c.ClaimedAt,
		&c.NotBefore, &c.CreatedAt, &c.PreparedAt, &c.AppliedAt)
	if err != nil {
		t.Fatalf("chunk %d: %v", chunkSeq, err)
	}
	return c
}

// mutRow rewrites the data object of every row of table whose data
// matches; rebundle then recomputes the content digest, so a crafted row
// reaches the import integrity checks instead of failing at the digest.
func mutRow(t *testing.T, table string, match func(map[string]any) bool, mutate func(map[string]any)) func(string) string {
	t.Helper()
	return func(line string) string {
		var rec map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return line
		}
		var name string
		if err := json.Unmarshal(rec["table"], &name); err != nil || name != table {
			return line
		}
		var data map[string]any
		if err := json.Unmarshal(rec["data"], &data); err != nil || !match(data) {
			return line
		}
		mutate(data)
		raw, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		rec["data"] = raw
		out, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		return string(out) + "\n"
	}
}

// rebundle rewrites row lines in an exported bundle and recomputes the
// content digest, so a deliberately bad row reaches the import integrity
// checks instead of failing at the digest.
func rebundle(t *testing.T, bundle []byte, fn func(line string) string) []byte {
	t.Helper()
	var out bytes.Buffer
	h := sha256.New()
	sc := bufio.NewScanner(bytes.NewReader(bundle))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		line := sc.Text() + "\n"
		if strings.Contains(line, `"record":"trailer"`) {
			var tr Trailer
			if err := json.Unmarshal([]byte(line), &tr); err != nil {
				t.Fatal(err)
			}
			tr.ContentSHA256 = hex.EncodeToString(h.Sum(nil))
			raw, _ := json.Marshal(tr)
			out.Write(append(raw, '\n'))
			continue
		}
		line = fn(line)
		h.Write([]byte(line))
		out.WriteString(line)
	}
	return out.Bytes()
}

// A secretary's memory is part of what moves: sealed ranges, accepted
// replacement text, kept and failed verdicts, prepared candidates, and the
// attempt/interruption history all cross the transfer with the journal they
// refer to. A live 'preparing' claim cannot cross — it belongs to the writer
// generation the seal fenced — so the cut returns that chunk to 'sealed' for
// the destination to claim under its own writer. The destination's first
// turn sees the applied fragments at their journal positions, settled
// verdicts are not re-litigated, and the carried work resumes where the
// source's authority ended.
func TestTransferCarriesSecretaryMemory(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "Moving secretary")))

	// Seven inputs each commit one ~11k-token record: MemoryMaintain seals
	// six chunks (one per input) and leaves the seventh input's records as
	// the live tail.
	big := strings.Repeat("remembered detail ", 2500)
	gen := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute)).Generation
	must(local.state.Recover(ctx, pid, gen))
	for i := 1; i <= 7; i++ {
		in := fmt.Sprintf("mem-%d", i)
		submit(t, local, pid, in, fmt.Sprintf("day %d", i))
		must(local.state.LoadTurn(ctx, pid, gen, fmt.Sprintf("mt-%d", i), 50))
		must(local.state.CommitTurn(ctx, pid, fmt.Sprintf("mt-%d", i), gen, agentstate.CommitRequest{
			Outcome: "complete",
			Events: []agentstate.EventInput{
				{Kind: "input_received", Payload: map[string]any{
					"input_id": in, "kind": "message", "text": fmt.Sprintf("day %d", i),
					"actor_kind": "human", "source_surface": "test", "attempt": 1,
				}},
				{Kind: "note", Payload: map[string]any{"text": big, "day": i}},
			},
			Output: map[string]any{"text": "ok"},
		}))
	}
	st := must(local.state.MemoryMaintain(ctx, pid, gen))
	if st.Sealed != 6 {
		t.Fatalf("maintain sealed %d chunks, want 6 (status %+v)", st.Sealed, st)
	}

	// Drive each lifecycle state through the real service.
	claimSeq(t, local, pid, gen, 1)
	must(local.state.CompleteMemoryChunk(ctx, pid, gen, 1, "Day one, compressed.", false))
	if st = must(local.state.MemoryMaintain(ctx, pid, gen)); st.Applied != 1 {
		t.Fatalf("chunk 1 did not apply: %+v", st)
	}
	claimSeq(t, local, pid, gen, 2)
	must(local.state.CompleteMemoryChunk(ctx, pid, gen, 2, "", true)) // kept
	claimSeq(t, local, pid, gen, 3)
	must(local.state.FailMemoryChunk(ctx, pid, gen, 3, "provider timeout", true))
	claimSeq(t, local, pid, gen, 4) // chunk 3 is backed off; 4 is next
	must(local.state.CompleteMemoryChunk(ctx, pid, gen, 4, "Day four, compressed.", false))
	// Chunk 3's backoff must be on the row; then push its deadline into
	// the past directly — not_before is wall-clock and a host clock step
	// (observed on this WSL2 host) can regress now() below a recorded
	// deadline, which is a clock artifact, not the eligibility contract.
	if c := chunkRow(t, local, pid, 3); c.NotBefore == nil {
		t.Fatalf("chunk 3 lost its first-attempt backoff: %+v", c)
	}
	if _, err := local.pool.Exec(ctx,
		`UPDATE core_memory_chunks SET not_before = '2000-01-01'::timestamptz
		 WHERE persona_id = $1 AND chunk_seq = 3`, pid); err != nil {
		t.Fatal(err)
	}
	claimSeq(t, local, pid, gen, 3)
	must(local.state.FailMemoryChunk(ctx, pid, gen, 3, "replacement rejected: factually wrong", false))
	// Chunk 5's claim is orphaned once (an interruption), then reclaimed —
	// its history crosses the transfer. Chunk 6 is still claimed when the
	// seal lands: the cut normalizes that dead claim back to 'sealed'.
	claimSeq(t, local, pid, gen, 5)
	claimSeq(t, local, pid, gen, 6)

	before := must(local.state.MemoryStatus(ctx, pid))
	if before.Applied != 1 || before.Kept != 1 || before.Failed != 1 ||
		before.Prepared != 1 || before.Preparing != 1 || before.Sealed != 1 {
		t.Fatalf("source memory shape %+v", before)
	}

	cloudID := placementID(t, cloud)
	rec := must(local.svc.Seal(ctx, pid, "move-mem", cloudID))
	want := Continuity{JournalEvents: 14, Notes: 7, UndeliveredOut: 7, MemoryApplied: 1,
		MemoryPrepared: 1, MemorySealed: 2, MemoryKept: 1, MemoryFailed: 1}
	if rec.Continuity != want {
		t.Fatalf("seal continuity = %+v, want %+v", rec.Continuity, want)
	}
	// The sealed source shows the cut shape: the dead claim is released.
	if c := chunkRow(t, local, pid, 6); c.Status != "sealed" || c.ClaimedGeneration != nil || c.ClaimedAt != nil {
		t.Fatalf("chunk 6 after seal: %+v", c)
	}
	if c := chunkRow(t, local, pid, 5); c.Status != "sealed" || c.Interruptions != 1 || c.NotBefore == nil {
		t.Fatalf("chunk 5 should keep its interruption pacing: %+v", c)
	}
	if c := chunkRow(t, local, pid, 3); c.Status != "failed" || c.Attempts != 2 {
		t.Fatalf("chunk 3 verdict: %+v", c)
	}
	bundle, exported := exportBytes(t, local, pid, "move-mem")

	humanID := newID(t)
	if _, err := cloud.pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, humanID); err != nil {
		t.Fatal(err)
	}
	staged, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID, false)
	if err != nil || !created {
		t.Fatalf("import: created=%v err=%v", created, err)
	}
	if staged.Continuity != want {
		t.Fatalf("staged continuity %+v, want %+v", staged.Continuity, want)
	}
	if !bytes.Equal(rowLines(t, local, pid), rowLines(t, cloud, pid)) {
		t.Fatal("destination rows differ from source rows")
	}
	must(cloud.svc.Activate(ctx, pid, "move-mem"))

	dgen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, dgen))

	// The destination's first turn already sees the applied fragment at its
	// journal position — not the raw originals it covers.
	submit(t, cloud, pid, "mem-8", "first cloud message")
	load := must(cloud.state.LoadTurn(ctx, pid, dgen, "ct-1", 50))
	if len(load.Memory) != 1 || load.Memory[0].ChunkSeq != 1 || load.Memory[0].Text != "Day one, compressed." {
		t.Fatalf("first destination turn memory %+v", load.Memory)
	}
	for _, e := range load.Context {
		if e.Seq >= load.Memory[0].FirstSeq && e.Seq <= load.Memory[0].LastSeq {
			t.Fatalf("covered original seq %d rendered alongside its applied block", e.Seq)
		}
	}
	must(cloud.state.CommitTurn(ctx, pid, "ct-1", dgen, agentstate.CommitRequest{
		Outcome: "complete", Output: map[string]any{"text": "hi"},
	}))

	// Maintenance continues the carried lifecycle: the shelved candidate
	// applies because live raw still exceeds the limit; settled verdicts are
	// untouched.
	dst := must(cloud.state.MemoryMaintain(ctx, pid, dgen))
	if dst.Applied != 2 || dst.Kept != 1 || dst.Failed != 1 || dst.Prepared != 0 {
		t.Fatalf("destination maintain %+v", dst)
	}
	if c := chunkRow(t, cloud, pid, 4); c.Status != "applied" || c.AppliedAt == nil {
		t.Fatalf("carried prepared chunk 4: %+v", c)
	}
	if c := chunkRow(t, cloud, pid, 2); c.Status != "kept" || c.PreparedAt == nil {
		t.Fatalf("kept verdict was re-litigated: %+v", c)
	}
	if c := chunkRow(t, cloud, pid, 3); c.Status != "failed" || c.Attempts != 2 ||
		c.LastError == nil || !strings.Contains(*c.LastError, "factually wrong") {
		t.Fatalf("failed verdict changed on the destination: %+v", c)
	}

	// Chunk 5's carried interruption pacing must still be on the row;
	// then push its deadline into the past directly rather than sleeping —
	// not_before is wall-clock and a host clock step could keep the claim
	// deferred, which is a clock artifact, not the contract under test.
	if c := chunkRow(t, cloud, pid, 5); c.NotBefore == nil {
		t.Fatalf("chunk 5 lost its carried interruption pacing: %+v", c)
	}
	if _, err := cloud.pool.Exec(ctx,
		`UPDATE core_memory_chunks SET not_before = '2000-01-01'::timestamptz
		 WHERE persona_id = $1 AND chunk_seq = 5`, pid); err != nil {
		t.Fatal(err)
	}
	claimSeq(t, cloud, pid, dgen, 5)
	must(cloud.state.CompleteMemoryChunk(ctx, pid, dgen, 5, "Day five, compressed.", false))
	if c := chunkRow(t, cloud, pid, 5); c.Status != "prepared" || c.Interruptions != 1 {
		t.Fatalf("chunk 5 after destination preparation: %+v", c)
	}
	// The normalized chunk 6 is ordinary sealed work for the destination.
	claimSeq(t, cloud, pid, dgen, 6)
	must(cloud.state.FailMemoryChunk(ctx, pid, dgen, 6, "cloud provider timeout", true))
	if c := chunkRow(t, cloud, pid, 6); c.Status != "sealed" || c.Attempts != 1 || c.Interruptions != 0 {
		t.Fatalf("normalized chunk 6 resumed with wrong history: %+v", c)
	}

	// The source stayed fenced and unchanged: the exported chunk rows still
	// hold exactly what the cut recorded, and no writer can start.
	if a := authority(t, local, pid); a != "sealed" {
		t.Fatalf("source authority %s", a)
	}
	if _, err := local.state.ClaimMemoryChunk(ctx, pid, gen, 50); !errors.Is(err, agentstate.ErrGenerationFence) {
		t.Fatalf("source memory claim after seal: %v, want fenced", err)
	}
	must(local.svc.Complete(ctx, pid, "move-mem",
		must(cloud.svc.Status(ctx, "import", "move-mem")).ActivateProof))
	if a := authority(t, local, pid); a != "transferred" {
		t.Fatalf("source authority %s after complete", a)
	}
	if staged.ContentSHA256 != exported.ContentSHA256 {
		t.Fatal("staged receipt does not match the export")
	}
}

// Upper-layer memory carries its full provenance: the superseded source
// rows (accepted decisions, texts and ranges durable), the applied L2 block
// at its sources' position, and an unfinished upper-layer preparation whose
// live claim cannot cross — the cut returns it to 'sealed' so the
// destination's own writer reclaims it.
func TestTransferCarriesUpperMemory(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "Moving secretary")))

	// Four small exchanges = seqs 1..8, then chunk rows in the shapes the
	// upper pipeline writes: two L1 sources superseded by an applied L2
	// block, two applied L1 fragments, and one L2 reintegration target
	// still claimed when the cut lands.
	gen := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute)).Generation
	must(local.state.Recover(ctx, pid, gen))
	for i := 1; i <= 4; i++ {
		in := fmt.Sprintf("u-%d", i)
		submit(t, local, pid, in, fmt.Sprintf("topic %d", i))
		must(local.state.LoadTurn(ctx, pid, gen, fmt.Sprintf("ut-%d", i), 50))
		must(local.state.CommitTurn(ctx, pid, fmt.Sprintf("ut-%d", i), gen, agentstate.CommitRequest{
			Outcome: "complete",
			Events: []agentstate.EventInput{
				{Kind: "input_received", Payload: map[string]any{
					"input_id": in, "kind": "message", "text": fmt.Sprintf("topic %d", i),
					"actor_kind": "human", "source_surface": "test", "attempt": 1,
				}},
				{Kind: "assistant_message", Payload: map[string]any{"text": fmt.Sprintf("answer %d", i)}},
			},
			Output: map[string]any{"text": "ok"},
		}))
	}
	if _, err := local.pool.Exec(ctx, `
		INSERT INTO core_memory_chunks
			(persona_id, chunk_seq, layer, sources, first_seq, last_seq, est_tokens,
			 status, replacement, replacement_est_tokens,
			 claimed_generation, claimed_at)
		VALUES
			($1, 1, 1, NULL, 1, 2, 300, 'superseded', 'L1 of days one', 60, NULL, NULL),
			($1, 2, 1, NULL, 3, 4, 300, 'superseded', 'L1 of days two', 60, NULL, NULL),
			($1, 3, 1, NULL, 5, 6, 300, 'applied', 'L1 of days three', 60, NULL, NULL),
			($1, 4, 1, NULL, 7, 8, 300, 'applied', 'L1 of days four', 60, NULL, NULL),
			($1, 5, 2, '{1,2}', 1, 4, 120, 'applied', 'L2: days one and two', 50, NULL, NULL),
			($1, 6, 2, '{3,4}', 5, 8, 120, 'preparing', NULL, NULL, $2, now())`,
		pid, gen); err != nil {
		t.Fatalf("seed upper state: %v", err)
	}

	cloudID := placementID(t, cloud)
	must(local.svc.Seal(ctx, pid, "move-upper", cloudID))
	// The preparing L2 target's claim was dead placement-local execution:
	// the cut returned it to 'sealed' for the destination to reclaim.
	if c := chunkRow(t, local, pid, 6); c.Status != "sealed" || c.ClaimedGeneration != nil {
		t.Fatalf("preparing L2 target after seal: %+v", c)
	}
	bundle, _ := exportBytes(t, local, pid, "move-upper")

	humanID := newID(t)
	if _, err := cloud.pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, humanID); err != nil {
		t.Fatal(err)
	}
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID, false); err != nil || !created {
		t.Fatalf("import: created=%v err=%v", created, err)
	}
	// Rows — including sources, superseded verdicts and the normalized
	// sealed target — arrive byte for byte.
	if !bytes.Equal(rowLines(t, local, pid), rowLines(t, cloud, pid)) {
		t.Fatal("destination rows differ from source rows")
	}
	must(cloud.svc.Activate(ctx, pid, "move-upper"))
	dgen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, dgen))

	// The destination renders the applied L2 block at its earliest source's
	// journal position and never the superseded coverage raw.
	load := must(cloud.state.LoadTurn(ctx, pid, dgen, "ct-1", 50))
	if len(load.Memory) != 3 ||
		load.Memory[0].ChunkSeq != 5 || load.Memory[0].Layer != 2 ||
		load.Memory[0].Text != "L2: days one and two" {
		t.Fatalf("destination memory view: %+v", load.Memory)
	}
	for _, e := range load.Context {
		if e.Seq <= 4 {
			t.Fatalf("superseded coverage seq %d rendered raw on the destination", e.Seq)
		}
	}

	// The carried sealed L2 target is ordinary work for the destination's
	// writer: claiming it resolves the carried sources to their fragments.
	claimed := must(cloud.state.ClaimMemoryChunk(ctx, pid, dgen, 50))
	if claimed.Chunk == nil || claimed.Chunk.ChunkSeq != 6 || claimed.Chunk.Layer != 2 {
		t.Fatalf("destination upper claim: %+v", claimed.Chunk)
	}
	if len(claimed.TargetFragments) != 2 || len(claimed.TargetEvents) != 0 ||
		claimed.TargetFragments[0].Text != "L1 of days three" {
		t.Fatalf("carried sources unresolved: %+v", claimed.TargetFragments)
	}
}

// An upper-layer row can pass the shape checks and still be a state the
// honest pipeline cannot emit: an applied target whose sources were never
// superseded (the range would render twice), or a source list with
// correct endpoints but an interior hole (journal coverage consumed by
// nothing). The seed below is the legitimate counterpart — an applied L2
// block over superseded L1s, a failed earlier attempt whose sources a
// later target consumed, and a sealed in-flight target over an applied
// source — and it must keep crossing.
func TestImportRefusesContradictoryUpperMemory(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "Moving secretary")))
	gen := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute)).Generation
	must(local.state.Recover(ctx, pid, gen))
	for i := 1; i <= 4; i++ {
		in := fmt.Sprintf("c-%d", i)
		submit(t, local, pid, in, fmt.Sprintf("topic %d", i))
		must(local.state.LoadTurn(ctx, pid, gen, fmt.Sprintf("ct-%d", i), 50))
		must(local.state.CommitTurn(ctx, pid, fmt.Sprintf("ct-%d", i), gen, agentstate.CommitRequest{
			Outcome: "complete",
			Events: []agentstate.EventInput{
				{Kind: "input_received", Payload: map[string]any{
					"input_id": in, "kind": "message", "text": fmt.Sprintf("topic %d", i),
					"actor_kind": "human", "source_surface": "test", "attempt": 1,
				}},
				{Kind: "assistant_message", Payload: map[string]any{"text": fmt.Sprintf("answer %d", i)}},
			},
			Output: map[string]any{"text": "ok"},
		}))
	}
	// A real history: an earlier consolidation over {1,2} failed
	// terminally, a later attempt reintegrated {1,2,3} into the applied
	// L2 block, and chunk 7 is a sealed reintegration target whose source
	// is still applied. Every row is a state the pipeline itself writes.
	if _, err := local.pool.Exec(ctx, `
		INSERT INTO core_memory_chunks
			(persona_id, chunk_seq, layer, sources, first_seq, last_seq, est_tokens,
			 status, replacement, replacement_est_tokens, attempts, last_error)
		VALUES
			($1, 1, 1, NULL, 1, 2, 300, 'superseded', 'L1 of days one', 60, 0, NULL),
			($1, 2, 1, NULL, 3, 4, 300, 'superseded', 'L1 of days two', 60, 0, NULL),
			($1, 3, 1, NULL, 5, 6, 300, 'superseded', 'L1 of days three', 60, 0, NULL),
			($1, 4, 1, NULL, 7, 8, 300, 'applied', 'L1 of days four', 60, 0, NULL),
			($1, 5, 2, '{1,2,3}', 1, 6, 180, 'applied', 'L2: days one through three', 70, 0, NULL),
			($1, 6, 2, '{1,2}', 1, 4, 120, 'failed', NULL, NULL, 3, 'provider refused'),
			($1, 7, 2, '{4}', 7, 8, 120, 'sealed', NULL, NULL, 0, NULL)`,
		pid); err != nil {
		t.Fatalf("seed upper state: %v", err)
	}
	must(local.svc.Seal(ctx, pid, "move-contra", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-contra")

	chunk := func(seq int64) func(map[string]any) bool {
		return func(d map[string]any) bool {
			v, ok := d["chunk_seq"].(float64)
			return ok && int64(v) == seq
		}
	}
	cases := map[string]struct {
		fn   func(string) string
		want string
	}{
		// The applied L2 target's sources are still applied — the same
		// journal range would render as both L1 fragments and the L2 block.
		"applied target over a live source": {mutRow(t, "core_memory_chunks", chunk(1),
			func(d map[string]any) { d["status"] = "applied" }), "memory_chunk_sources_invalid"},
		// Sources {1,3} keep the target's endpoints right ([1,6]) but leave
		// [3,4] — chunk 2's range — inside the claimed span yet named by no
		// source.
		"source list with an interior gap": {mutRow(t, "core_memory_chunks", chunk(5),
			func(d map[string]any) { d["sources"] = []any{1, 3} }), "memory_chunk_sources_invalid"},
		// Overlapping instead of gapped: widening chunk 1 to [1,4] makes
		// the sources cover [1,6] twice over [3,4] with the target bounds
		// unchanged.
		"overlapping source ranges": {mutRow(t, "core_memory_chunks", chunk(1),
			func(d map[string]any) { d["last_seq"] = 4 }), "memory_chunk_sources_invalid"},
		// The sources tile the range but are listed out of order — the
		// settled-tuple dedup compares arrays order-sensitively, so this
		// tuple would not match the canonical one it was settled as.
		"reordered settled source tuple": {mutRow(t, "core_memory_chunks", chunk(6),
			func(d map[string]any) { d["sources"] = []any{2, 1} }), "memory_chunk_sources_invalid"},
		// A source is only ever consumed while applied and only ever
		// retired as superseded: a sealed or failed source is a state the
		// pipeline cannot produce.
		"source still sealed": {mutRow(t, "core_memory_chunks", chunk(1),
			func(d map[string]any) { d["status"] = "sealed" }), "memory_chunk_sources_invalid"},
		// One source listed twice inflates the reference list without
		// resolving to two chunks.
		"repeated source reference": {mutRow(t, "core_memory_chunks", chunk(5),
			func(d map[string]any) { d["sources"] = []any{1, 1} }), "memory_chunk_sources_invalid"},
		// A target cannot consume across layers: {1,6} mixes an L1 source
		// with the failed L2 row.
		"cross-layer source": {mutRow(t, "core_memory_chunks", chunk(5),
			func(d map[string]any) { d["sources"] = []any{1, 6} }), "memory_chunk_sources_invalid"},
		// Chunk 5 demoted to 'failed' leaves chunks 1-3 superseded with no
		// applied ancestor anywhere — their journal range renders as
		// nothing on the destination.
		"superseded coverage no applied ancestor": {mutRow(t, "core_memory_chunks", chunk(5),
			func(d map[string]any) { d["status"] = "failed" }), "memory_chunk_superseded_uncovered"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			bad := rebundle(t, bundle, tc.fn)
			_, _, err := cloud.svc.Import(ctx, bytes.NewReader(bad), nil, false)
			if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("import err = %v, want ErrIntegrity mentioning %q", err, tc.want)
			}
			if _, err := cloud.state.PersonaState(ctx, pid); !errors.Is(err, agentstate.ErrPersonaNotFound) {
				t.Fatalf("a refused bundle left a persona behind: %v", err)
			}
		})
	}

	// The legitimate history still crosses: the failed target keeps its
	// provenance over sources another target consumed, the sealed target
	// keeps its applied source, and the destination activates and renders
	// the applied upper block without double-rendering.
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); err != nil || !created {
		t.Fatalf("legitimate upper history import: created=%v err=%v", created, err)
	}
	if !bytes.Equal(rowLines(t, local, pid), rowLines(t, cloud, pid)) {
		t.Fatal("destination rows differ from source rows")
	}
	must(cloud.svc.Activate(ctx, pid, "move-contra"))
	dgen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, dgen))
	load := must(cloud.state.LoadTurn(ctx, pid, dgen, "ct-d1", 50))
	if len(load.Memory) != 2 ||
		load.Memory[0].ChunkSeq != 5 || load.Memory[0].Text != "L2: days one through three" ||
		load.Memory[1].ChunkSeq != 4 || load.Memory[1].Text != "L1 of days four" {
		t.Fatalf("destination memory view: %+v", load.Memory)
	}
	for _, e := range load.Context {
		t.Fatalf("superseded coverage seq %d rendered raw on the destination", e.Seq)
	}
	// The sealed reintegration target is claimable at the destination and
	// resolves its still-applied source fragment.
	claimed := must(cloud.state.ClaimMemoryChunk(ctx, pid, dgen, 50))
	if claimed.Chunk == nil || claimed.Chunk.ChunkSeq != 7 ||
		len(claimed.TargetFragments) != 1 || claimed.TargetFragments[0].Text != "L1 of days four" {
		t.Fatalf("destination claim on carried sealed target: %+v", claimed.Chunk)
	}
}

// addChunkRow appends one crafted core_memory_chunks row after the table's
// last row (preserving the row order the importer requires) and recomputes
// the trailer's row count and content digest, so the row reaches import
// verification instead of failing at the digest.
func addChunkRow(t *testing.T, bundle []byte, data map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	line := fmt.Sprintf(
		`{"record":"row","section":"core","table":"core_memory_chunks","data":%s}`+"\n",
		raw)
	sc := bufio.NewScanner(bytes.NewReader(bundle))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text()+"\n")
	}
	lastTableLine := -1
	for i, l := range lines {
		if strings.Contains(l, `"table":"core_memory_chunks"`) {
			lastTableLine = i
		}
	}
	if lastTableLine < 0 {
		t.Fatal("bundle carries no core_memory_chunks rows to extend")
	}
	var tr Trailer
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &tr); err != nil || tr.Record != "trailer" {
		t.Fatal("last bundle line is not a trailer")
	}
	var out bytes.Buffer
	h := sha256.New()
	for i, l := range lines[:len(lines)-1] {
		out.WriteString(l)
		h.Write([]byte(l))
		if i == lastTableLine {
			out.WriteString(line)
			h.Write([]byte(line))
		}
	}
	tr.Rows["core_memory_chunks"]++
	tr.ContentSHA256 = hex.EncodeToString(h.Sum(nil))
	tl, err := json.Marshal(tr)
	if err != nil {
		t.Fatal(err)
	}
	out.Write(append(tl, '\n'))
	return out.Bytes()
}

// Coverage the pipeline cannot emit: chunk ranges tile disjointly — an L1
// range is allocated once and stays covered even by a failed verdict, and
// applying a target supersedes exactly its sources. A second L1 row over
// already-covered seqs, or two applied rows sharing a seq (the range would
// render twice), can only come from a crafted bundle.
func TestImportRefusesOverlappingCoverage(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "Moving secretary")))
	gen := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute)).Generation
	must(local.state.Recover(ctx, pid, gen))
	for i := 1; i <= 4; i++ {
		in := fmt.Sprintf("o-%d", i)
		submit(t, local, pid, in, fmt.Sprintf("topic %d", i))
		must(local.state.LoadTurn(ctx, pid, gen, fmt.Sprintf("ot-%d", i), 50))
		must(local.state.CommitTurn(ctx, pid, fmt.Sprintf("ot-%d", i), gen, agentstate.CommitRequest{
			Outcome: "complete",
			Events: []agentstate.EventInput{
				{Kind: "input_received", Payload: map[string]any{
					"input_id": in, "kind": "message", "text": fmt.Sprintf("topic %d", i),
					"actor_kind": "human", "source_surface": "test", "attempt": 1,
				}},
				{Kind: "assistant_message", Payload: map[string]any{"text": fmt.Sprintf("answer %d", i)}},
			},
			Output: map[string]any{"text": "ok"},
		}))
	}
	if _, err := local.pool.Exec(ctx, `
		INSERT INTO core_memory_chunks
			(persona_id, chunk_seq, layer, sources, first_seq, last_seq, est_tokens,
			 status, replacement, replacement_est_tokens)
		VALUES
			($1, 1, 1, NULL, 1, 2, 300, 'superseded', 'L1 of days one', 60),
			($1, 2, 1, NULL, 3, 4, 300, 'superseded', 'L1 of days two', 60),
			($1, 3, 1, NULL, 5, 6, 300, 'applied', 'L1 of days three', 60),
			($1, 4, 1, NULL, 7, 8, 300, 'applied', 'L1 of days four', 60),
			($1, 5, 2, '{1,2}', 1, 4, 120, 'applied', 'L2: days one and two', 50)`,
		pid); err != nil {
		t.Fatalf("seed upper state: %v", err)
	}
	must(local.svc.Seal(ctx, pid, "move-overlap", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-overlap")

	ghost := func(status string) map[string]any {
		return map[string]any{
			"persona_id": pid, "chunk_seq": 9, "layer": 1, "sources": nil,
			"first_seq": 2, "last_seq": 3, "est_tokens": 100,
			"status": status, "replacement": nil, "replacement_est_tokens": nil,
			"attempts": 0, "interruptions": 0, "last_error": nil,
			"claimed_generation": nil, "claimed_at": nil, "not_before": nil,
			"created_at":  "2026-09-15T00:00:00Z",
			"prepared_at": nil, "applied_at": nil,
		}
	}

	// A sealed L1 over already-covered seqs is pipeline-impossible — the
	// destination's own pipeline would claim, prepare and apply it into a
	// real applied overlap.
	t.Run("sealed L1 over covered range", func(t *testing.T) {
		bad := addChunkRow(t, bundle, ghost("sealed"))
		_, _, err := cloud.svc.Import(ctx, bytes.NewReader(bad), nil, false)
		if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), "memory_chunk_l1_overlap") {
			t.Fatalf("import err = %v, want ErrIntegrity mentioning memory_chunk_l1_overlap", err)
		}
	})

	// An applied L1 inside an applied L2 target's coverage double-renders
	// the range in every sent view.
	t.Run("applied L1 inside applied coverage", func(t *testing.T) {
		g := ghost("applied")
		g["replacement"] = "ghost fragment"
		g["replacement_est_tokens"] = 30
		g["prepared_at"] = "2026-09-15T00:00:00Z"
		g["applied_at"] = "2026-09-15T00:00:00Z"
		bad := addChunkRow(t, bundle, g)
		_, _, err := cloud.svc.Import(ctx, bytes.NewReader(bad), nil, false)
		if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), "memory_chunk_applied_overlap") {
			t.Fatalf("import err = %v, want ErrIntegrity mentioning memory_chunk_applied_overlap", err)
		}
	})

	// The clean bundle still imports: disjoint coverage with a legitimate
	// applied target over superseded sources.
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); err != nil || !created {
		t.Fatalf("legitimate history import: created=%v err=%v", created, err)
	}
}

// A carried chunk can never hold a live claim, and its range must resolve
// inside the carried journal. Both are integrity violations a valid digest
// cannot launder.
func TestImportRefusesMalformedMemoryChunks(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "Moving secretary")))
	gen := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute)).Generation
	submit(t, local, pid, "m-1", "hi")
	must(local.state.LoadTurn(ctx, pid, gen, "t-1", 50))
	must(local.state.CommitTurn(ctx, pid, "t-1", gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events: []agentstate.EventInput{
			{Kind: "input_received", Payload: map[string]any{
				"input_id": "m-1", "kind": "message", "text": "hi",
				"actor_kind": "human", "source_surface": "test", "attempt": 1,
			}},
			{Kind: "note", Payload: map[string]any{"text": strings.Repeat("x", 45000)}},
		},
	}))
	must(local.state.MemoryMaintain(ctx, pid, gen))
	// One sealed chunk is not enough — the seal walk needs a following input
	// boundary. Add a second input so the first chunk seals.
	submit(t, local, pid, "m-2", "again")
	must(local.state.LoadTurn(ctx, pid, gen, "t-2", 50))
	must(local.state.CommitTurn(ctx, pid, "t-2", gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events: []agentstate.EventInput{
			{Kind: "input_received", Payload: map[string]any{
				"input_id": "m-2", "kind": "message", "text": "again",
				"actor_kind": "human", "source_surface": "test", "attempt": 1,
			}},
			{Kind: "note", Payload: map[string]any{"text": "second"}},
		},
	}))
	must(local.state.MemoryMaintain(ctx, pid, gen))
	var sealedChunks int64
	if err := local.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_memory_chunks WHERE persona_id = $1 AND status = 'sealed'`,
		pid).Scan(&sealedChunks); err != nil || sealedChunks != 1 {
		t.Fatalf("sealed chunks %d err %v, want 1", sealedChunks, err)
	}
	// A queued input carries received_seq NULL — the legitimate unjournaled
	// state that must survive the marker checks.
	submit(t, local, pid, "m-3", "queued")

	must(local.svc.Seal(ctx, pid, "move-badmem", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-badmem")

	// Every case below is a crafted/recomputed-digest bundle — state an
	// honest store cannot emit — which is exactly the class verifyCut
	// exists for.
	chunkRow := func(d map[string]any) bool { return d["chunk_seq"] != nil }
	inputRow := func(id string) func(map[string]any) bool {
		return func(d map[string]any) bool { return d["input_id"] == id }
	}
	set := func(k string, v any) func(map[string]any) {
		return func(d map[string]any) { d[k] = v }
	}

	cases := map[string]struct {
		fn   func(line string) string
		want string
	}{
		"carried claim": {func(line string) string {
			if !strings.Contains(line, `"table":"core_memory_chunks"`) {
				return line
			}
			line = strings.Replace(line, `"status": "sealed"`, `"status": "preparing"`, 1)
			return strings.Replace(line, `"claimed_generation": null`, `"claimed_generation": 5`, 1)
		}, "memory_chunk_claim_carried"},
		"range past the journal": {func(line string) string {
			if !strings.Contains(line, `"table":"core_memory_chunks"`) {
				return line
			}
			return strings.Replace(line, `"last_seq": 2`, `"last_seq": 99`, 1)
		}, "memory_chunk_range_outside_journal"},
		"unknown status": {func(line string) string {
			if !strings.Contains(line, `"table":"core_memory_chunks"`) {
				return line
			}
			return strings.Replace(line, `"status": "sealed"`, `"status": "bogus"`, 1)
		}, ""},
		// f70/F-B1: a prepared/applied row without replacement text (or its
		// estimate) stages today and bricks every destination LoadTurn on the
		// appliedBlocks scan; the prepared shape is latent until maintain
		// applies it. kept is checked separately: keep-unchanged stores both
		// NULL, non-shrinking keeps both set — one without the other is not a
		// row the store writes.
		"applied without replacement": {mutRow(t, "core_memory_chunks", chunkRow,
			set("status", "applied")), "memory_chunk_missing_payload"},
		"prepared without replacement": {mutRow(t, "core_memory_chunks", chunkRow,
			set("status", "prepared")), "memory_chunk_missing_payload"},
		"kept text without estimate": {mutRow(t, "core_memory_chunks", chunkRow, func(d map[string]any) {
			d["status"] = "kept"
			d["replacement"] = "condensed"
		}), "memory_chunk_missing_payload"},
		// f71/F2: negative sequence/counter values corrupt ordering and the
		// live-raw accounting while staging cleanly.
		"negative chunk_seq": {mutRow(t, "core_memory_chunks", chunkRow,
			set("chunk_seq", -7)), "memory_chunk_negative_values"},
		"negative est_tokens": {mutRow(t, "core_memory_chunks", chunkRow,
			set("est_tokens", -900000)), "memory_chunk_negative_values"},
		"layer zero": {mutRow(t, "core_memory_chunks", chunkRow,
			set("layer", 0)), "memory_chunk_negative_values"},
		// Upper-layer provenance: a layer-1 row never carries sources; a
		// layer-2 target's sources must all resolve to carried chunks whose
		// span is exactly the target's range.
		"layer-1 row carrying sources": {mutRow(t, "core_memory_chunks", chunkRow,
			set("sources", []any{1})), "memory_chunk_sources_invalid"},
		"upper target with a dangling source": {mutRow(t, "core_memory_chunks", chunkRow,
			func(d map[string]any) {
				d["layer"] = 2
				d["sources"] = []any{999}
			}), "memory_chunk_sources_invalid"},
		"upper target with an empty source set": {mutRow(t, "core_memory_chunks", chunkRow,
			func(d map[string]any) {
				d["layer"] = 2
				d["sources"] = []any{}
			}), "memory_chunk_sources_invalid"},
		// F-B2: received_seq that dangles, names the wrong kind, or names
		// another input's event would make the destination skip journaling
		// this input's input_received — its original record silently lost.
		"received_seq another input's event": {mutRow(t, "core_inputs", inputRow("m-2"),
			set("received_seq", 1)), "input_received_seq_mismatch"},
		"received_seq wrong kind": {mutRow(t, "core_inputs", inputRow("m-2"),
			set("received_seq", 2)), "input_received_seq_mismatch"},
		"received_seq dangling on queued input": {mutRow(t, "core_inputs", inputRow("m-3"),
			set("received_seq", 99)), "input_received_seq_mismatch"},
		// The reverse link: m-2's input_received exists in the journal, so a
		// missing marker would let the destination journal it a second time.
		"received_seq marker removed": {mutRow(t, "core_inputs", inputRow("m-2"),
			set("received_seq", nil)), "input_received_seq_not_linked"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			bad := rebundle(t, bundle, tc.fn)
			_, _, err := cloud.svc.Import(ctx, bytes.NewReader(bad), nil, false)
			if err == nil {
				t.Fatal("import accepted a malformed memory chunk")
			}
			if tc.want != "" && (!errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("import err = %v, want ErrIntegrity mentioning %q", err, tc.want)
			}
			if _, err := cloud.state.PersonaState(ctx, pid); !errors.Is(err, agentstate.ErrPersonaNotFound) {
				t.Fatalf("a refused bundle left a persona behind: %v", err)
			}
		})
	}
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); err != nil || !created {
		t.Fatalf("clean import after refusals: created=%v err=%v", created, err)
	}
	// Bounds are floors, not a layer policy: the queued input's NULL marker
	// must still cross (the clean import above proves it). But an upper-layer
	// row now has a contract — consolidation is real: a layer-2 chunk
	// without resolvable sources is malformed, not "future work". Re-address
	// the mutated bundle to a second placement since the first already holds
	// this transfer under the clean digest.
	cloud2 := newPlacement(t)
	sourceless := rebundle(t, bundle, func(line string) string {
		line = strings.Replace(line, placementID(t, cloud), placementID(t, cloud2), 1)
		return mutRow(t, "core_memory_chunks", chunkRow, set("layer", 2))(line)
	})
	if _, _, err := cloud2.svc.Import(ctx, bytes.NewReader(sourceless), nil, false); err == nil ||
		!errors.Is(err, ErrIntegrity) ||
		!strings.Contains(err.Error(), "memory_chunk_sources_invalid") {
		t.Fatalf("sourceless layer-2 chunk: %v, want memory_chunk_sources_invalid", err)
	}
}

// f74: the Go CommitTurn API accepts a request carrying the same
// input_received twice — the store dedups at the write boundary, so an
// accepted commit can never produce the unlinked second receipt that
// input_received_seq_not_linked would refuse at every later Seal.
func TestSealAfterStoreProducedDuplicateReceipt(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "Dupe receipt")))
	gen := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute)).Generation
	submit(t, local, pid, "d-1", "hello")
	must(local.state.LoadTurn(ctx, pid, gen, "t-1", 50))
	must(local.state.CommitTurn(ctx, pid, "t-1", gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events: []agentstate.EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "d-1"}},
			{Kind: "input_received", Payload: map[string]any{"input_id": "d-1"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "hi"}},
		},
		Output: map[string]any{"text": "hi"},
	}))
	var receipts int64
	if err := local.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_events WHERE persona_id = $1 AND kind = 'input_received'`,
		pid).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("journaled receipts = %d err=%v, want 1", receipts, err)
	}
	// The accepted state must be movable: seal, export, import all succeed.
	must(local.svc.Seal(ctx, pid, "move-dupe", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-dupe")
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); err != nil || !created {
		t.Fatalf("import after duplicate-receipt commit: created=%v err=%v", created, err)
	}
}

// f-memory-86/87, end to end: a commit carrying a receipt for an absent
// input is refused atomically — nothing is journaled, the turn survives for
// a valid retry — and the persona stays transferable, keeping a continued
// conversation on the destination.
func TestRejectedGhostReceiptKeepsPersonaTransferable(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "Ghost receipt")))
	gen := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute)).Generation
	submit(t, local, pid, "in-1", "hello")
	must(local.state.LoadTurn(ctx, pid, gen, "t-1", 50))
	// A receipt naming no input row is refused; a non-string id too.
	for _, payload := range []map[string]any{
		{"input_id": "ghost-1"},
		{"input_id": float64(5)},
	} {
		if _, err := local.state.CommitTurn(ctx, pid, "t-1", gen, agentstate.CommitRequest{
			Outcome: "complete",
			Events: []agentstate.EventInput{
				{Kind: "input_received", Payload: map[string]any{"input_id": "in-1"}},
				{Kind: "input_received", Payload: payload},
				{Kind: "assistant_message", Payload: map[string]any{"text": "hi"}},
			},
			Output: map[string]any{"text": "hi"},
		}); !errors.Is(err, agentstate.ErrBadRequest) {
			t.Fatalf("commit with receipt %v: err = %v, want ErrBadRequest", payload, err)
		}
	}
	var events int64
	if err := local.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_events WHERE persona_id = $1`, pid).Scan(&events); err != nil || events != 0 {
		t.Fatalf("events after refused commits = %d err=%v, want 0", events, err)
	}
	// The turn is still running and commits once the receipts are valid.
	must(local.state.CommitTurn(ctx, pid, "t-1", gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events: []agentstate.EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-1"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "hi"}},
		},
		Output: map[string]any{"text": "hi"},
	}))
	submit(t, local, pid, "in-2", "second")
	load := must(local.state.LoadTurn(ctx, pid, gen, "t-2", 50))
	if load.Input == nil || load.Input.InputID != "in-2" {
		t.Fatalf("t-2 claimed %+v, want in-2", load.Input)
	}
	must(local.state.CommitTurn(ctx, pid, "t-2", gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events: []agentstate.EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-2"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "second reply"}},
		},
		Output: map[string]any{"text": "second reply"},
	}))
	must(local.svc.Seal(ctx, pid, "move-ghost", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-ghost")
	humanID := newID(t)
	must(cloud.pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, humanID))
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID, false); err != nil || !created {
		t.Fatalf("import after refused ghost receipt: created=%v err=%v", created, err)
	}
	act := must(cloud.svc.Activate(ctx, pid, "move-ghost"))
	must(local.svc.Complete(ctx, pid, "move-ghost", act.ActivateProof))
	// Continued conversation on the destination.
	cgen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	submit(t, cloud, pid, "in-cloud", "hello from the cloud")
	load = must(cloud.state.LoadTurn(ctx, pid, cgen, "ct-1", 50))
	if load.Input == nil || load.Input.InputID != "in-cloud" {
		t.Fatalf("destination turn claimed %+v, want in-cloud", load.Input)
	}
	must(cloud.state.CommitTurn(ctx, pid, "ct-1", cgen, agentstate.CommitRequest{
		Outcome: "complete",
		Events: []agentstate.EventInput{
			{Kind: "input_received", Payload: map[string]any{"input_id": "in-cloud"}},
			{Kind: "assistant_message", Payload: map[string]any{"text": "hi cloud"}},
		},
		Output: map[string]any{"text": "hi cloud"},
	}))
	var receipts int64
	if err := cloud.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_events WHERE persona_id = $1 AND kind = 'input_received'`,
		pid).Scan(&receipts); err != nil || receipts != 3 {
		t.Fatalf("destination receipts = %d err=%v, want 3", receipts, err)
	}
	var unlinked int64
	if err := cloud.pool.QueryRow(ctx, `
		SELECT count(*) FROM core_events e
		JOIN core_inputs i ON i.persona_id = e.persona_id
			AND i.input_id = e.payload->>'input_id'
		WHERE e.persona_id = $1 AND e.kind = 'input_received'
			AND (i.received_seq IS NULL OR i.received_seq <> e.seq)`,
		pid).Scan(&unlinked); err != nil || unlinked != 0 {
		t.Fatalf("destination unlinked receipts = %d err=%v, want 0", unlinked, err)
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
			_, _, err := cloud.svc.Import(ctx, bytes.NewReader(tc.bundle), nil, false)
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
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); err != nil || !created {
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
	_, _, err := cloud.svc.Import(ctx, bytes.NewReader(out.Bytes()), nil, false)
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
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); err != nil {
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
	// Each iteration gets its own placement, but as a subtest: a finished
	// iteration's pool and database are dropped at once instead of twenty-
	// four of them accumulating against the shared Postgres until the whole
	// test cleans up.
	for i := 0; i < 24; i++ {
		t.Run(fmt.Sprintf("race-%d", i), func(t *testing.T) {
			ctx := context.Background()
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
				_, err := local.svc.Seal(ctx, pid, fmt.Sprintf("move-race-%d", i), destination)
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
				t.Fatalf("seal committed yet the job it checked for was queued")
			case sErr == nil:
				if !errors.Is(jErr, agentstate.ErrPersonaInactive) {
					t.Fatalf("submit after seal committed: %v, want persona inactive", jErr)
				}
				if queued {
					t.Fatalf("refused submit left a job row")
				}
			case errors.Is(sErr, ErrUnresolvedOperations):
				if jErr != nil || !queued {
					t.Fatalf("seal refused for the job but submit err=%v queued=%v", jErr, queued)
				}
			default:
				t.Fatalf("unexpected seal=%v submit=%v queued=%v", sErr, jErr, queued)
			}
		})
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
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(first), nil, false)))

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
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(first), nil, false); !errors.Is(err, ErrTransferConflict) {
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
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(second), nil, false)))
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(first), nil, false); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("importing the retired cut beside the staged one: %v, want conflict", err)
	}
	// A different transfer's bundle for a persona already present is refused.
	stranger := bytes.Replace(second, []byte(`"transfer_id":"move-0006"`), []byte(`"transfer_id":"move-0006b"`), 1)
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(stranger), nil, false); !errors.Is(err, ErrPersonaExists) {
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

// admission_seq is backed by one table-global identity sequence. The import
// regenerates it: every staged row's value comes from the destination's own
// nextval in bundle order, so no carried value can collide with or rewind
// the destination's allocations, and the work is bounded by the number of
// transferred records — a bundle carrying huge admission_seq values from a
// long-lived source must not force a billion sequence bumps. Regression for
// the populated-destination finding f-shared-intake-97.
func TestImportIntoPopulatedDestinationKeepsAdmissionOrder(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)

	seqPos := func() (int64, bool) {
		var last int64
		var called bool
		if err := cloud.pool.QueryRow(ctx,
			`SELECT last_value, is_called FROM core_inputs_admission_seq_seq`).Scan(&last, &called); err != nil {
			t.Fatalf("sequence position: %v", err)
		}
		return last, called
	}
	globalMax := func() int64 {
		var m int64
		if err := cloud.pool.QueryRow(ctx,
			`SELECT COALESCE(max(admission_seq), 0) FROM core_inputs`).Scan(&m); err != nil {
			t.Fatalf("table max: %v", err)
		}
		return m
	}
	dupes := func() int {
		var n int
		if err := cloud.pool.QueryRow(ctx, `
			SELECT count(*) FROM (
				SELECT 1 FROM core_inputs
				GROUP BY persona_id, admission_seq HAVING count(*) > 1) d`).Scan(&n); err != nil {
			t.Fatalf("duplicate check: %v", err)
		}
		return n
	}

	// The destination already hosts a persona whose inputs outrank what the
	// bundle will carry.
	other := newID(t)
	must(drop(cloud.state.EnsurePersona(ctx, other, nil, "Staying secretary")))
	for i := 1; i <= 4; i++ {
		submit(t, cloud, other, fmt.Sprintf("pre-%d", i), fmt.Sprintf("existing %d", i))
	}

	// The moved persona carries a smaller history.
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "Moving secretary")))
	submit(t, local, pid, "m-1", "first")
	submit(t, local, pid, "m-2", "second")
	must(local.svc.Seal(ctx, pid, "move-seq", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-seq")

	// While the import runs, real admissions to the staying persona race it.
	// The interleaving is deliberately not pinned — every interleaving must
	// be safe, so the assertions check the outcome, not a schedule.
	var wg sync.WaitGroup
	subErr := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := cloud.state.SubmitInput(ctx, &agentstate.Input{
				PersonaID: other, InputID: fmt.Sprintf("race-%d", i), Kind: "message",
				Payload: map[string]any{"text": "racing"}, ActorKind: "human", SourceSurface: "test",
			})
			subErr <- err
		}(i)
	}
	humanID := newID(t)
	if _, err := cloud.pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, humanID); err != nil {
		t.Fatal(err)
	}
	_, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID, false)
	if err != nil || !created {
		t.Fatalf("import: created=%v err=%v", created, err)
	}
	wg.Wait()
	close(subErr)
	for err := range subErr {
		if err != nil {
			t.Fatalf("concurrent admission during import: %v", err)
		}
	}
	if n := dupes(); n != 0 {
		t.Fatalf("%d same-persona duplicate admission_seq values after import", n)
	}
	last, _ := seqPos()
	if last < globalMax() {
		t.Fatalf("identity sequence at %d below table max %d after import", last, globalMax())
	}
	// The moved persona's claim order is the source's admission order, over
	// destination-allocated values.
	var order []string
	rows, err := cloud.pool.Query(ctx,
		`SELECT input_id FROM core_inputs WHERE persona_id = $1 ORDER BY admission_seq`, pid)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		order = append(order, id)
	}
	rows.Close()
	if fmt.Sprint(order) != "[m-1 m-2]" {
		t.Fatalf("destination claim order %v, want [m-1 m-2]", order)
	}
	// Every other column carried verbatim across a populated destination.
	// The source's admission_seq values (1, 2) and the destination's rebased
	// ones genuinely differ here, so this comparison fails unless rowLines
	// actually normalizes the identity column out of the row data.
	if !bytes.Equal(rowLines(t, local, pid), rowLines(t, cloud, pid)) {
		t.Fatal("destination rows differ from source rows")
	}

	// The next admission to the staying persona orders after everything.
	submit(t, cloud, other, "post-import", "after the move")
	var seq int64
	if err := cloud.pool.QueryRow(ctx,
		`SELECT admission_seq FROM core_inputs WHERE persona_id = $1 AND input_id = 'post-import'`,
		other).Scan(&seq); err != nil {
		t.Fatalf("post-import seq: %v", err)
	}
	if seq <= last {
		t.Fatalf("post-import admission_seq %d did not pass sequence position %d", seq, last)
	}
	if n := dupes(); n != 0 {
		t.Fatalf("%d same-persona duplicate admission_seq values after post-import admission", n)
	}

	// A rejected bundle can consume at most one nextval per staged row
	// (the digest is verified after the rows stream in) — a legal gap,
	// never a rewind.
	before, _ := seqPos()
	corrupt := bytes.Clone(bundle)
	corrupt[len(corrupt)-40] ^= 0xFF
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(corrupt), &humanID, false); err == nil {
		t.Fatal("corrupted bundle imported")
	}
	if after, _ := seqPos(); after < before {
		t.Fatalf("rejected import rewound the sequence %d -> %d", before, after)
	}

	// A bundle carrying huge admission_seq values — e.g. exported from a
	// long-lived multi-persona source — costs a bounded number of sequence
	// allocations: one per carried row, independent of the numeric gap.
	// rewriteBundleAdmissionSeqs decodes each row, swaps the carried value,
	// and recomputes the trailer digest, so the ~4e9 values below are what
	// the importer actually checks and stages — it fails rather than degrade
	// to a no-op if the bundle's byte shape changes.
	big := newID(t)
	must(drop(local.state.EnsurePersona(ctx, big, nil, "Long-lived secretary")))
	submit(t, local, big, "b-1", "old input")
	submit(t, local, big, "b-2", "newer input")
	must(local.svc.Seal(ctx, big, "move-big", placementID(t, cloud)))
	bigBundle, _ := exportBytes(t, local, big, "move-big")
	bigBundle = rewriteBundleAdmissionSeqs(t, bigBundle, []int64{4000000003, 4000000004})
	before, _ = seqPos()
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bigBundle), &humanID, false); err != nil || !created {
		t.Fatalf("big-seq import: created=%v err=%v", created, err)
	}
	after, _ := seqPos()
	if after-before > 10 {
		t.Fatalf("import advanced the sequence by %d for a 2-row bundle — work is proportional to the numeric gap, not the data", after-before)
	}
	var bigSeqs []int64
	rows, err = cloud.pool.Query(ctx,
		`SELECT admission_seq FROM core_inputs WHERE persona_id = $1 ORDER BY admission_seq`, big)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s int64
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		bigSeqs = append(bigSeqs, s)
	}
	rows.Close()
	if len(bigSeqs) != 2 || bigSeqs[0] < 1 || bigSeqs[1] <= bigSeqs[0] || bigSeqs[0] > 4000000000 {
		t.Fatalf("rebased admission_seqs %v: want fresh small values in source order", bigSeqs)
	}

	// An empty carried persona (no inputs at all) consumes nothing.
	empty := newID(t)
	must(drop(local.state.EnsurePersona(ctx, empty, nil, "Empty secretary")))
	must(local.svc.Seal(ctx, empty, "move-empty", placementID(t, cloud)))
	emptyBundle, _ := exportBytes(t, local, empty, "move-empty")
	before, _ = seqPos()
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(emptyBundle), &humanID, false); err != nil || !created {
		t.Fatalf("empty import: created=%v err=%v", created, err)
	}
	if after, _ := seqPos(); after != before {
		t.Fatalf("empty import moved the sequence %d -> %d", before, after)
	}

	// The staying persona still passes the cut's own integrity checks — its
	// admission order was never corrupted.
	if _, err := cloud.svc.Seal(ctx, other, "move-away", placementID(t, local)); err != nil {
		t.Fatalf("seal of pre-existing destination persona after import: %v", err)
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
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))

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
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))

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
	if _, _, err := cloudB.svc.Import(ctx, bytes.NewReader(bundle), nil, false); !errors.Is(err, ErrBadBundle) ||
		!strings.Contains(err.Error(), "addressed to placement") {
		t.Fatalf("import on the wrong placement: %v", err)
	}
	must(drop(cloudA.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))
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
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); !errors.Is(err, ErrTransferConflict) {
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
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))

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
		_, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)
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
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(dup), nil, false); !errors.Is(err, ErrBadBundle) ||
		!strings.Contains(err.Error(), "duplicates a key") {
		t.Fatalf("duplicate-key bundle: %v", err)
	}

	// A human_id that does not exist names the parameter, not the bundle.
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), ptr(newID(t)), false); !errors.Is(err, ErrBadRequest) ||
		!strings.Contains(err.Error(), "human_id") {
		t.Fatalf("missing human: %v", err)
	}

	humanID := newID(t)
	if _, err := cloud.pool.Exec(ctx, `INSERT INTO humans (human_id) VALUES ($1)`, humanID); err != nil {
		t.Fatal(err)
	}
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID, false)))
	// Replays must carry the same human_id; a different one is a conflict,
	// never a silent rebind.
	other := newID(t)
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &other, false); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("replay with a different human_id: %v", err)
	}
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &humanID, false); err != nil || created {
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
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))

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
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); !errors.Is(err, ErrTransferConflict) {
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
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("the real bundle staged beside a wrong-persona tombstone: %v", err)
	}
	fixed := must(cloud.svc.Retire(ctx, pid, "move-0022", cloudID, key))
	if fixed.PersonaID != pid {
		t.Fatalf("correction kept persona %s", fixed.PersonaID)
	}
	must(local.svc.Abort(ctx, pid, "move-0022", fixed.RetireProof))
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); !errors.Is(err, ErrTransferConflict) {
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
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))
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

// input returns the persona's input row (the turn half is unused here).
func input(t *testing.T, p placement, personaID, inputID string) agentstate.Input {
	t.Helper()
	in, _, err := p.state.GetInput(context.Background(), personaID, inputID)
	if err != nil {
		t.Fatalf("get input %s: %v", inputID, err)
	}
	return in
}

// mustExec runs a plain Exec and fails on error.
func mustExec(t *testing.T, p placement, sql string, args ...any) {
	t.Helper()
	if _, err := p.pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %s: %v", sql, err)
	}
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

// --- Approvals and wait accounting travel with the persona (M08 repair f43) ---

// parkedApproval builds a persona whose only input waits on a pending
// elevated message.send: the operation is awaiting_approval, the turn
// awaiting, the input waiting with waiting_since set. Returns the
// approval id.
func parkedApproval(t *testing.T, p placement, personaID, humanID string) string {
	t.Helper()
	ctx := context.Background()
	must(drop(p.state.EnsurePersona(ctx, personaID, &humanID, "secretary")))
	submit(t, p, personaID, "in-park", "please send the notice")
	gen := must(p.state.AcquireWriter(ctx, personaID, "local-core", time.Minute)).Generation
	load := must(p.state.LoadTurn(ctx, personaID, gen, "turn-park", 50))
	if load.Input == nil || load.Input.InputID != "in-park" {
		t.Fatalf("load %+v", load.Input)
	}
	send := agentstate.PlanCall{Tool: "message.send", Route: "elevated",
		Request: map[string]any{"text": "moving notice"}}
	must(drop(p.state.SavePlan(ctx, personaID, "turn-park", gen, 0,
		agentstate.Decision{Text: "needs consent", Calls: []agentstate.PlanCall{send}})))
	op, a, fresh, err := p.state.ClaimOperation(ctx, personaID, "turn-park", gen,
		"op-park-0", "message.send", 0, send.Request)
	if err != nil || !fresh || a == nil || a.Status != "pending" || op.Status != "awaiting_approval" {
		t.Fatalf("park claim: op=%+v a=%+v fresh=%v err=%v", op, a, fresh, err)
	}
	must(p.state.CommitTurn(ctx, personaID, "turn-park", gen,
		agentstate.CommitRequest{Outcome: "await"}))
	in := input(t, p, personaID, "in-park")
	if in.Status != "waiting" || in.WaitingSince == nil {
		t.Fatalf("parked input %+v", in)
	}
	return a.ApprovalID
}

// A pending approval is carried with its operation linkage and stays
// resolvable at the destination — but only for the destination-bound
// human: the source human's id and the carried record's provenance are
// not destination authority. The grant executes exactly once there.
func TestTransferCarriesAPendingApproval(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	srcHuman := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, srcHuman)
	apprID := parkedApproval(t, local, pid, srcHuman)

	rec := must(local.svc.Seal(ctx, pid, "move-appr", placementID(t, cloud)))
	if rec.Continuity.PendingApprovals != 1 || rec.Continuity.WaitingInputs != 1 {
		t.Fatalf("seal continuity %+v", rec.Continuity)
	}
	// Once sealed, the source takes no new decisions on it — the pending
	// approval belongs to the destination now.
	if _, err := local.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-src", DecidedByKind: "human", DecidedByID: srcHuman,
	}); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("decision on sealed source: %v, want inactive", err)
	}
	bundle, _ := exportBytes(t, local, pid, "move-appr")

	dstHuman := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, dstHuman)
	staged, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &dstHuman, false)
	if err != nil || !created {
		t.Fatalf("import: created=%v err=%v", created, err)
	}
	if staged.Continuity.PendingApprovals != 1 || staged.Continuity.WaitingInputs != 1 {
		t.Fatalf("import continuity %+v", staged.Continuity)
	}
	// The waiting input and its parked approval arrive intact.
	in := input(t, cloud, pid, "in-park")
	if in.Status != "waiting" || in.WaitingSince == nil {
		t.Fatalf("staged input %+v", in)
	}
	a := must(cloud.state.GetApproval(ctx, pid, apprID))
	if a.Status != "pending" || a.Decision != nil || a.DecidedByID != nil || a.ConsumedAt != nil {
		t.Fatalf("staged approval %+v", a)
	}
	// A staged persona takes no decisions.
	if _, err := cloud.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-x", DecidedByKind: "human", DecidedByID: dstHuman,
	}); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("decision on staged persona: %v, want inactive", err)
	}

	must(cloud.svc.Activate(ctx, pid, "move-appr"))
	// Neither the source human's id nor a stranger's decides at the
	// destination — only the human this placement bound at import.
	for _, who := range []string{srcHuman, newID(t)} {
		if _, err := cloud.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
			Decision: "approve_once", DecisionID: "d-" + who[:8], DecidedByKind: "human", DecidedByID: who,
		}); !errors.Is(err, agentstate.ErrApprovalForbidden) {
			t.Fatalf("decision by %s: %v, want forbidden", who, err)
		}
	}
	got := must(cloud.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-1", DecidedByKind: "human", DecidedByID: dstHuman,
	}))
	if got.Status != "approved" || got.Provenance == nil || *got.Provenance != "agent_own_with_human_consent" {
		t.Fatalf("destination decision %+v", got)
	}
	// The human's thinking time — including the span before the move —
	// accumulated into waited_ms instead of resetting.
	in = input(t, cloud, pid, "in-park")
	if in.Status != "queued" || in.WaitingSince != nil || in.WaitedMs <= 0 {
		t.Fatalf("requeued input %+v", in)
	}
	// The next writer runs the granted call exactly once.
	gen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, gen))
	load := must(cloud.state.LoadTurn(ctx, pid, gen, "turn-resume", 50))
	if load.Input == nil || load.Input.InputID != "in-park" {
		t.Fatalf("resume load %+v", load.Input)
	}
	op, grant, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-resume", gen,
		"op-resume-0", "message.send", 0, map[string]any{"text": "moving notice"})
	if err != nil || op.Status != "done" || !fresh || grant == nil || grant.ConsumedAt == nil {
		t.Fatalf("consume claim: op=%+v grant=%+v fresh=%v err=%v", op, grant, fresh, err)
	}
	// A replayed claim replays the receipt — no second effect.
	op2, _, fresh2, err := cloud.state.ClaimOperation(ctx, pid, "turn-resume", gen,
		"op-resume-0b", "message.send", 0, map[string]any{"text": "moving notice"})
	if err != nil || fresh2 || op2.Status != "done" || op2.OperationID != op.OperationID {
		t.Fatalf("replayed claim: op=%+v fresh=%v err=%v", op2, fresh2, err)
	}
	n := 0
	for _, e := range must(cloud.state.Outbox(ctx, pid, 0, 100)) {
		if e.Kind == "secretary_message" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("secretary_message outbox entries = %d, want exactly 1", n)
	}
	// A replayed decision returns the stored record; a divergent one conflicts.
	if again := must(cloud.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-1", DecidedByKind: "human", DecidedByID: dstHuman,
	})); again.Status != "approved" {
		t.Fatalf("decision replay %+v", again)
	}
	if _, err := cloud.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "deny_once", DecisionID: "d-2", DecidedByKind: "human", DecidedByID: dstHuman,
	}); !errors.Is(err, agentstate.ErrApprovalConflict) {
		t.Fatalf("divergent decision: %v, want conflict", err)
	}
}

// A denial decided at the source is preserved at the destination: the
// carried operation replays the recorded denial — it is never re-executed
// and never re-asked.
func TestTransferPreservesADenial(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	srcHuman := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, srcHuman)
	apprID := parkedApproval(t, local, pid, srcHuman)
	denied := must(local.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "deny_once", DecisionID: "d-deny", DecidedByKind: "human", DecidedByID: srcHuman,
	}))
	if denied.Status != "denied" {
		t.Fatalf("deny %+v", denied)
	}
	// The decision requeued the input; the parked op is failed with the
	// denial as its durable response.
	in := input(t, local, pid, "in-park")
	if in.Status != "queued" {
		t.Fatalf("input after denial %+v", in)
	}

	must(local.svc.Seal(ctx, pid, "move-deny", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-deny")
	dstHuman := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, dstHuman)
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), &dstHuman, false)))
	must(cloud.svc.Activate(ctx, pid, "move-deny"))

	// The decided record arrived with its provenance.
	a := must(cloud.state.GetApproval(ctx, pid, apprID))
	if a.Status != "denied" || a.Decision == nil || *a.Decision != "deny_once" ||
		a.DecidedByID == nil || *a.DecidedByID != srcHuman {
		t.Fatalf("carried denial %+v", a)
	}
	// The destination's writer replays the stored denial — no prompt, no send.
	gen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, gen))
	load := must(cloud.state.LoadTurn(ctx, pid, gen, "turn-deny", 50))
	if load.Input == nil || load.Input.InputID != "in-park" {
		t.Fatalf("deny resume load %+v", load.Input)
	}
	op, grant, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-deny", gen,
		"op-deny-0", "message.send", 0, map[string]any{"text": "moving notice"})
	if err != nil || fresh || op.Status != "failed" || grant == nil || grant.Status != "denied" {
		t.Fatalf("denied replay: op=%+v grant=%+v fresh=%v err=%v", op, grant, fresh, err)
	}
	for _, e := range must(cloud.state.Outbox(ctx, pid, 0, 100)) {
		if e.Kind == "secretary_message" {
			t.Fatal("denied message.send delivered at the destination")
		}
	}
	// No new approval was minted: the same one still answers.
	if l := must(cloud.state.ListApprovals(ctx, pid, "")); len(l) != 1 || l[0].ApprovalID != apprID {
		t.Fatalf("destination approvals %+v", l)
	}
}

// A configured user's selection no longer blocks the move: seal snapshots
// it onto the persona as non-secret model intent — explicit 'none' and an
// api selection's provider/model/base_url metadata alike — while no
// credential or unrelated person's selection travels. An unselected
// persona snapshots NULL and keeps ordinary unset semantics.
func TestSealCarriesModelIntent(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)

	// Explicit 'none' travels as intent — the destination must reproduce
	// the refusal to run a model, not silently take a default.
	pid := newID(t)
	human := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, human)
	must(drop(local.state.EnsurePersona(ctx, pid, &human, "secretary")))
	mustExec(t, local, `INSERT INTO model_connection_selections (human_id, kind) VALUES ($1, 'none')`, human)
	if _, err := local.svc.Seal(ctx, pid, "move-none", placementID(t, cloud)); err != nil {
		t.Fatalf("seal with a none selection: %v", err)
	}
	var intent []byte
	if err := local.pool.QueryRow(ctx,
		`SELECT model_intent FROM core_personas WHERE persona_id = $1`, pid).Scan(&intent); err != nil {
		t.Fatal(err)
	}
	var noneIntent map[string]any
	if err := json.Unmarshal(intent, &noneIntent); err != nil || noneIntent["kind"] != "none" {
		t.Fatalf("none intent %s", intent)
	}
	bundle, _ := exportBytes(t, local, pid, "move-none")
	if !bytes.Contains(bundle, []byte(`"model_intent"`)) {
		t.Fatal("bundle persona row does not carry model_intent")
	}
	dstHuman := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, dstHuman)
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), &dstHuman, false)))
	if err := cloud.pool.QueryRow(ctx,
		`SELECT model_intent FROM core_personas WHERE persona_id = $1`, pid).Scan(&intent); err != nil {
		t.Fatal(err)
	}
	noneIntent = nil
	if err := json.Unmarshal(intent, &noneIntent); err != nil || noneIntent["kind"] != "none" {
		t.Fatalf("carried none intent %s", intent)
	}

	// An api selection travels as kind + non-secret connection metadata;
	// the credential never appears in the bundle.
	pid2 := newID(t)
	human2 := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, human2)
	must(drop(local.state.EnsurePersona(ctx, pid2, &human2, "configured")))
	connID := uuid.NewString()
	mustExec(t, local, `INSERT INTO model_api_connections
		(human_id, connection_id, name, preset, base_url, model, credential_ciphertext, version)
		VALUES ($1, $2::uuid, 'work', 'openai-chat', 'https://api.example.test', 'model-9', '\x00'::bytea, $3::uuid)`,
		human2, connID, uuid.NewString())
	mustExec(t, local, `INSERT INTO model_connection_selections (human_id, kind, connection_id)
		VALUES ($1, 'api', $2::uuid)`, human2, connID)
	if _, err := local.svc.Seal(ctx, pid2, "move-api", placementID(t, cloud)); err != nil {
		t.Fatalf("seal with an api selection: %v", err)
	}
	if err := local.pool.QueryRow(ctx,
		`SELECT model_intent FROM core_personas WHERE persona_id = $1`, pid2).Scan(&intent); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(intent, &got); err != nil {
		t.Fatal(err)
	}
	conn, _ := got["connection"].(map[string]any)
	if got["kind"] != "api" || conn["preset"] != "openai-chat" ||
		conn["model"] != "model-9" || conn["base_url"] != "https://api.example.test" {
		t.Fatalf("api intent %s", intent)
	}
	apiBundle, _ := exportBytes(t, local, pid2, "move-api")
	if bytes.Contains(apiBundle, []byte("credential_ciphertext")) ||
		bytes.Contains(apiBundle, []byte("access_token")) {
		t.Fatal("credential material leaked into the bundle")
	}
	for _, line := range bytes.Split(apiBundle, []byte("\n")) {
		if !bytes.Contains(line, []byte(`"table":"core_personas"`)) {
			continue
		}
		var prow struct {
			Data struct {
				ModelIntent map[string]any `json:"model_intent"`
			} `json:"data"`
		}
		if err := json.Unmarshal(line, &prow); err != nil {
			t.Fatal(err)
		}
		for k := range prow.Data.ModelIntent {
			if strings.Contains(k, "credential") || strings.Contains(k, "token") {
				t.Fatalf("model_intent carries a secret field %q", k)
			}
		}
	}
	// No selection row may ride the bundle, and no other human's
	// selections are touched.
	if strings.Contains(string(apiBundle), "model_connection") {
		t.Fatal("selection rows must not be carried")
	}

	// An unbound/unselected persona snapshots no intent.
	pid3 := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid3, nil, "unbound")))
	if _, err := local.svc.Seal(ctx, pid3, "move-free", placementID(t, cloud)); err != nil {
		t.Fatalf("seal unbound persona: %v", err)
	}
	if err := local.pool.QueryRow(ctx,
		`SELECT model_intent FROM core_personas WHERE persona_id = $1`, pid3).Scan(&intent); err != nil {
		t.Fatal(err)
	}
	if intent != nil {
		t.Fatalf("unbound persona intent %s, want NULL", intent)
	}
}

// mutateTableRows rewrites the data object of every row line for a table
// and rehashes the trailer, producing a structurally valid bundle whose
// contents are incoherent.
func mutateTableRows(t *testing.T, bundle []byte, table string, fn func(data map[string]any)) []byte {
	t.Helper()
	return rehashBundle(t, mutateLines(bundle, func(lines []string) []string {
		for i, line := range lines {
			var row map[string]json.RawMessage
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				continue
			}
			var tbl string
			if err := json.Unmarshal(row["table"], &tbl); err != nil || tbl != table {
				continue
			}
			var data map[string]any
			if err := json.Unmarshal(row["data"], &data); err != nil {
				t.Fatal(err)
			}
			fn(data)
			raw, err := json.Marshal(data)
			if err != nil {
				t.Fatal(err)
			}
			row["data"] = raw
			out, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			lines[i] = string(out) + "\n"
		}
		return lines
	}))
}

// tableRow returns the decoded data of the row in table whose primary key
// field (approval_id, operation_id, persona_id, …) equals id.
func tableRow(t *testing.T, bundle []byte, table, id string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(string(bundle), "\n") {
		var row map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			continue
		}
		var tbl string
		if err := json.Unmarshal(row["table"], &tbl); err != nil || tbl != table {
			continue
		}
		var data map[string]any
		if err := json.Unmarshal(row["data"], &data); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"approval_id", "operation_id", "input_id", "persona_id"} {
			if v, ok := data[key]; ok && v == id {
				return data
			}
		}
	}
	t.Fatalf("no %s row for %s in bundle", table, id)
	return nil
}

// A grant approved at the source but not yet executed is consent under the
// source's account. Imported under a different human without a
// same-authority assertion, it is re-pended — the original decision kept
// in prior_* as provenance — and the destination's bound human decides
// before the effect runs once.
func TestApprovedGrantRependsAcrossAuthority(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	srcHuman := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, srcHuman)
	apprID := parkedApproval(t, local, pid, srcHuman)
	// The source human consents; the send never ran before the move.
	must(local.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-src", DecidedByKind: "human", DecidedByID: srcHuman,
	}))

	must(local.svc.Seal(ctx, pid, "move-grant", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-grant")
	dstHuman := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, dstHuman)
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), &dstHuman, false)))

	// The grant did not cross: the approval is pending again, carrying the
	// source decision as provenance only.
	a := must(cloud.state.GetApproval(ctx, pid, apprID))
	if a.Status != "pending" || a.Decision != nil || a.DecidedByID != nil {
		t.Fatalf("re-pended grant %+v", a)
	}
	if a.PriorDecision == nil || *a.PriorDecision != "approve_once" ||
		a.PriorDecidedByID == nil || *a.PriorDecidedByID != srcHuman || a.PriorDecidedAt == nil {
		t.Fatalf("provenance not preserved %+v", a)
	}
	must(cloud.svc.Activate(ctx, pid, "move-grant"))
	// The parked op must not execute under the source's consent.
	gen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, gen))
	load := must(cloud.state.LoadTurn(ctx, pid, gen, "turn-rp", 50))
	if load.Input == nil || load.Input.InputID != "in-park" {
		t.Fatalf("resume load %+v", load.Input)
	}
	// Replaying the parked call must not execute: the op stays awaiting and
	// the re-pended grant is handed back for a destination decision.
	op, grant, _, err := cloud.state.ClaimOperation(ctx, pid, "turn-rp", gen,
		"op-rp-0", "message.send", 0, map[string]any{"text": "moving notice"})
	if err != nil || grant == nil || grant.Status != "pending" || op.Status != "awaiting_approval" {
		t.Fatalf("claim under re-pended grant: op=%+v grant=%+v err=%v", op, grant, err)
	}
	must(cloud.state.CommitTurn(ctx, pid, "turn-rp", gen, agentstate.CommitRequest{Outcome: "await"}))
	for _, e := range must(cloud.state.Outbox(ctx, pid, 0, 100)) {
		if e.Kind == "secretary_message" {
			t.Fatal("the send executed without a destination decision")
		}
	}
	// The source human's id is not destination authority.
	if _, err := cloud.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-src2", DecidedByKind: "human", DecidedByID: srcHuman,
	}); !errors.Is(err, agentstate.ErrApprovalForbidden) {
		t.Fatalf("source-human decision at destination: %v, want forbidden", err)
	}
	// The bound destination human decides; the call executes exactly once.
	must(cloud.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-dst", DecidedByKind: "human", DecidedByID: dstHuman,
	}))
	load = must(cloud.state.LoadTurn(ctx, pid, gen, "turn-rp2", 50))
	if load.Input == nil {
		t.Fatal("requeued input did not resume")
	}
	op2, grant2, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-rp2", gen,
		"op-rp-1", "message.send", 0, map[string]any{"text": "moving notice"})
	if err != nil || !fresh || op2.Status != "done" || grant2 == nil || grant2.ConsumedAt == nil {
		t.Fatalf("consume after destination decision: op=%+v grant=%+v err=%v", op2, grant2, err)
	}
}

// With an explicit same-authority assertion, a source grant that was never
// consumed continues at the destination and executes once — no second
// prompt. decided_by stays provenance; the assertion, not the bundle's
// fields, is what carries the authority.
func TestApprovedGrantContinuesWithSameHuman(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	srcHuman := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, srcHuman)
	apprID := parkedApproval(t, local, pid, srcHuman)
	must(local.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-src", DecidedByKind: "human", DecidedByID: srcHuman,
	}))

	must(local.svc.Seal(ctx, pid, "move-same", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-same")
	dstHuman := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, dstHuman)
	staged, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &dstHuman, true)
	if err != nil || !created {
		t.Fatalf("same-human import: created=%v err=%v", created, err)
	}
	if !staged.SameHuman {
		t.Fatalf("receipt does not record the same_human assertion: %+v", staged)
	}
	a := must(cloud.state.GetApproval(ctx, pid, apprID))
	if a.Status != "approved" || a.DecidedByID == nil || *a.DecidedByID != srcHuman || a.ConsumedAt != nil {
		t.Fatalf("same-authority grant %+v", a)
	}
	// same_human requires a bound human to assert against.
	pidB := newID(t)
	srcB := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, srcB)
	parkedApproval(t, local, pidB, srcB)
	must(local.svc.Seal(ctx, pidB, "move-same-b", placementID(t, cloud)))
	bundleB, _ := exportBytes(t, local, pidB, "move-same-b")
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundleB), nil, true); !errors.Is(err, ErrBadRequest) {
		t.Fatalf("same_human without human_id: %v, want bad request", err)
	}
	// A replay asserting differently conflicts — the staged authority
	// decision is fixed.
	if _, _, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), &dstHuman, false); !errors.Is(err, ErrTransferConflict) {
		t.Fatalf("replay dropping the assertion: %v, want conflict", err)
	}

	must(cloud.svc.Activate(ctx, pid, "move-same"))
	gen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, gen))
	load := must(cloud.state.LoadTurn(ctx, pid, gen, "turn-sh", 50))
	if load.Input == nil || load.Input.InputID != "in-park" {
		t.Fatalf("resume load %+v", load.Input)
	}
	// No new prompt: the grant consumes into the one execution.
	op, grant, fresh, err := cloud.state.ClaimOperation(ctx, pid, "turn-sh", gen,
		"op-sh-0", "message.send", 0, map[string]any{"text": "moving notice"})
	if err != nil || !fresh || op.Status != "done" || grant == nil || grant.ConsumedAt == nil {
		t.Fatalf("grant continuation: op=%+v grant=%+v err=%v", op, grant, err)
	}
}

// retargetTransfer rewrites a mutated bundle's transfer_id so independent
// cases do not collide on the ledger's one-content-per-transfer rule.
func retargetTransfer(t *testing.T, bundle []byte, id string) []byte {
	t.Helper()
	return rehashBundle(t, mutateLines(bundle, func(lines []string) []string {
		var hdr map[string]json.RawMessage
		if err := json.Unmarshal([]byte(lines[0]), &hdr); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(id)
		if err != nil {
			t.Fatal(err)
		}
		hdr["transfer_id"] = raw
		out, err := json.Marshal(hdr)
		if err != nil {
			t.Fatal(err)
		}
		lines[0] = string(out) + "\n"
		return lines
	}))
}

// An impossible approval cut is refused transactionally — at the source's
// seal and at the destination's import — leaving no partially staged
// persona. Approvals and their parked operations must agree on the exact
// action and on a reachable lifecycle state.
func TestImportRejectsIncoherentApprovalCuts(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	srcHuman := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, srcHuman)
	parkedApproval(t, local, pid, srcHuman)
	must(local.svc.Seal(ctx, pid, "move-coh", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-coh")

	cases := map[string]struct {
		bundle []byte
		want   string
	}{
		// The human saw one request while the operation would run another.
		"approval request differs from the operation's": {
			mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
				d["request"] = map[string]any{"text": "a different action entirely"}
			}), "approval_operation_request_mismatch"},
		// A digest that does not recompute from tool+route+request.
		"forged action digest": {
			mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
				d["action_digest"] = strings.Repeat("0", 64)
			}), "approval_digest_mismatch"},
		// A denial attached to an operation still awaiting it — the
		// reviewers' infinite re-park loop.
		"denied approval on an awaiting operation": {
			mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
				d["status"] = "denied"
				d["decision"] = "deny_once"
				d["decision_id"] = "d-forge"
				d["decided_by_kind"] = "human"
				d["decided_by_id"] = srcHuman
				d["decided_at"] = "2026-09-14T00:00:00Z"
			}), "denied_approval_operation_not_failed"},
		// An already-spent grant on an operation that could run again.
		"consumed grant on an awaiting operation": {
			mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
				d["status"] = "approved"
				d["decision"] = "approve_once"
				d["decision_id"] = "d-forge"
				d["decided_by_kind"] = "human"
				d["decided_by_id"] = srcHuman
				d["decided_at"] = "2026-09-14T00:00:00Z"
				d["provenance"] = "agent_own_with_human_consent"
				d["consumed_at"] = "2026-09-14T00:01:00Z"
			}), "consumed_grant_operation_unsettled"},
		// A pending approval carrying a consumption mark.
		"pending approval marked consumed": {
			mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
				d["consumed_at"] = "2026-09-14T00:00:00Z"
			}), "pending_approval_decided_fields"},
		// A decided approval missing who decided it.
		"decided approval without a decider": {
			mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
				d["status"] = "approved"
				d["decision"] = "approve_once"
				d["decision_id"] = "d-forge"
				d["decided_at"] = "2026-09-14T00:00:00Z"
				d["provenance"] = "agent_own_with_human_consent"
			}), "decided_approval_incomplete"},
		// The parked operation settled while its approval stayed pending.
		"pending approval on a finished operation": {
			mutateTableRows(t, bundle, "core_operations", func(d map[string]any) {
				if d["status"] == "awaiting_approval" {
					d["status"] = "done"
				}
			}), "pending_approval_operation_resolved"},
		// An awaiting operation whose only approval is already spent —
		// nothing left that can resolve it.
		"awaiting operation with no live approval": {
			mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
				d["status"] = "approved"
				d["decision"] = "approve_once"
				d["decision_id"] = "d-forge"
				d["decided_by_kind"] = "human"
				d["decided_by_id"] = srcHuman
				d["decided_at"] = "2026-09-14T00:00:00Z"
				d["provenance"] = "agent_own_with_human_consent"
				d["consumed_at"] = "2026-09-14T00:01:00Z"
			}), "awaiting_operation_without_live_approval"},
	}
	n := 0
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			n++
			tid := fmt.Sprintf("move-coh-%02d", n)
			_, _, err := cloud.svc.Import(ctx, bytes.NewReader(retargetTransfer(t, tc.bundle, tid)), nil, false)
			if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("import err = %v, want ErrIntegrity mentioning %q", err, tc.want)
			}
			// Transactional refusal: no staged persona, no ledger row.
			if _, err := cloud.state.PersonaState(ctx, pid); !errors.Is(err, agentstate.ErrPersonaNotFound) {
				t.Fatalf("a refused cut left a persona behind: %v", err)
			}
			if _, err := cloud.svc.Status(ctx, "import", tid); !errors.Is(err, ErrTransferNotFound) {
				t.Fatalf("a refused cut left a ledger row: %v", err)
			}
		})
	}
	// The source applies the same matrix at seal: corrupt the stored cut
	// and the seal refuses instead of exporting a broken bundle.
	pid2 := newID(t)
	src2 := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, src2)
	parkedApproval(t, local, pid2, src2)
	mustExec(t, local, `UPDATE core_tool_approvals SET request = '{"text":"forged"}'::jsonb WHERE persona_id = $1`, pid2)
	if _, err := local.svc.Seal(ctx, pid2, "move-coh-src", placementID(t, cloud)); !errors.Is(err, ErrIntegrity) ||
		!strings.Contains(err.Error(), "approval_operation_request_mismatch") {
		t.Fatalf("seal over an incoherent cut: %v", err)
	}
}

// An import without a human is not a trap: a staged persona with a pending
// approval cannot activate — nothing may decide for it — until the
// admin binds a human, after which activation, the decision and the one
// execution all proceed. An unbound persona with nothing pending
// activates normally; no secretary is required to have an employer.
func TestUnboundImportBindsAndActivates(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	srcHuman := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, srcHuman)
	apprID := parkedApproval(t, local, pid, srcHuman)
	must(local.svc.Seal(ctx, pid, "move-unbound", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-unbound")

	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))
	// Activation refuses truthfully rather than stranding the waiting
	// input on a decider that does not exist.
	if _, err := cloud.svc.Activate(ctx, pid, "move-unbound"); !errors.Is(err, ErrTransferConflict) ||
		!strings.Contains(err.Error(), "bind a human") {
		t.Fatalf("activate unbound with a pending approval: %v", err)
	}
	// Binding is reachable while staged.
	dstHuman := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, dstHuman)
	p := must(cloud.state.BindHuman(ctx, pid, dstHuman))
	if p.HumanID == nil || *p.HumanID != dstHuman {
		t.Fatalf("bind %+v", p)
	}
	// A second bind to another human conflicts — no silent rebind.
	other := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, other)
	if _, err := cloud.state.BindHuman(ctx, pid, other); !errors.Is(err, agentstate.ErrPersonaBound) {
		t.Fatalf("rebind: %v, want bound conflict", err)
	}
	must(cloud.svc.Activate(ctx, pid, "move-unbound"))
	must(cloud.state.ResolveApproval(ctx, pid, apprID, agentstate.ApprovalDecision{
		Decision: "approve_once", DecisionID: "d-b", DecidedByKind: "human", DecidedByID: dstHuman,
	}))
	gen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, gen))
	load := must(cloud.state.LoadTurn(ctx, pid, gen, "turn-ub", 50))
	if load.Input == nil {
		t.Fatal("waiting input did not resume after bind")
	}
	op, grant, _, err := cloud.state.ClaimOperation(ctx, pid, "turn-ub", gen,
		"op-ub-0", "message.send", 0, map[string]any{"text": "moving notice"})
	if err != nil || op.Status != "done" || grant == nil || grant.ConsumedAt == nil {
		t.Fatalf("post-bind consume: op=%+v grant=%+v err=%v", op, grant, err)
	}

	// No pending approvals → an unbound persona activates and can bind
	// later while active. Not every secretary needs an employer; only
	// capabilities needing a decider do.
	pid2 := newID(t)
	liveSecretary(t, local, pid2)
	must(local.svc.Seal(ctx, pid2, "move-free", placementID(t, cloud)))
	free, _ := exportBytes(t, local, pid2, "move-free")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(free), nil, false)))
	must(cloud.svc.Activate(ctx, pid2, "move-free"))
	if _, err := cloud.state.BindHuman(ctx, pid2, dstHuman); err != nil {
		t.Fatalf("late bind on an active unbound persona: %v", err)
	}
	// EnsurePersona over the bound persona is honest: the same human is
	// idempotent, a different one conflicts rather than silently reporting
	// success over an unchanged owner.
	existing, created, err := cloud.state.EnsurePersona(ctx, pid2, &dstHuman, "x")
	if err != nil || created || existing.HumanID == nil || *existing.HumanID != dstHuman {
		t.Fatalf("ensure over bound persona: %+v created=%v err=%v", existing, created, err)
	}
	stranger := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, stranger)
	if _, _, err := cloud.state.EnsurePersona(ctx, pid2, &stranger, "x"); !errors.Is(err, agentstate.ErrPersonaBound) {
		t.Fatalf("ensure with a different human: %v, want bound conflict", err)
	}
}

// --- Abort/clear lifecycle: intent restore and sealed-cut fence (f75/f76) ---

// modelIntent reads the persona's carried intent column.
func modelIntent(t *testing.T, p placement, personaID string) json.RawMessage {
	t.Helper()
	var intent json.RawMessage
	if err := p.pool.QueryRow(context.Background(),
		`SELECT model_intent FROM core_personas WHERE persona_id = $1`, personaID).Scan(&intent); err != nil {
		t.Fatal(err)
	}
	return intent
}

// binding resolves the persona's model binding through the real HTTP
// handler on a metadata-only connection store.
func binding(t *testing.T, p placement, personaID string) (int, agentstate.ModelBinding) {
	t.Helper()
	srv := agentstate.NewServer(p.pool, "portable-test-admin-secret")
	srv.SetModelConnections(modelconnections.MetadataOnly(p.pool))
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	req := httptest.NewRequest("GET", "/internal/core/personas/"+personaID+"/model", nil)
	req.Header.Set("Authorization", "Bearer "+srv.PersonaToken(personaID))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	var b agentstate.ModelBinding
	if rec.Code == 200 {
		if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
			t.Fatal(err)
		}
	}
	return rec.Code, b
}

// A cancelled transfer restores the source's pre-seal model semantics: the
// intent snapshot is a transfer artifact, so a persona that never moved
// gets NULL back (its live selection decides again), while a persona that
// itself arrived carrying an unresolved intent keeps that intent — the
// obligation it still owes.
func TestAbortRestoresSourceModelIntent(t *testing.T) {
	ctx := context.Background()
	local, cloud, edge := newPlacement(t), newPlacement(t), newPlacement(t)

	pid := newID(t)
	human := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, human)
	must(drop(local.state.EnsurePersona(ctx, pid, &human, "never-moved")))
	mustExec(t, local, `INSERT INTO model_connection_selections (human_id, kind) VALUES ($1, 'none')`, human)
	sealed := must(local.svc.Seal(ctx, pid, "move-abort-1", placementID(t, cloud)))
	var intent map[string]any
	if err := json.Unmarshal(modelIntent(t, local, pid), &intent); err != nil || intent["kind"] != "none" {
		t.Fatalf("sealed intent %s", modelIntent(t, local, pid))
	}
	if len(sealed.PriorModelIntent) != 0 {
		t.Fatalf("a never-moved persona recorded prior intent %s", sealed.PriorModelIntent)
	}
	bundle, _ := exportBytes(t, local, pid, "move-abort-1")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))
	retired := retireDest(t, cloud, pid, "move-abort-1", "")
	must(local.svc.Abort(ctx, pid, "move-abort-1", retired.RetireProof))
	if got := modelIntent(t, local, pid); got != nil {
		t.Fatalf("abort left intent %s on a persona that never moved", got)
	}
	// The human now picks an api connection — an ordinary account change.
	// With the transfer artifact gone the binding resolves it; nothing is
	// stranded in needs_rebinding.
	connID := uuid.NewString()
	mustExec(t, local, `INSERT INTO model_api_connections
		(human_id, connection_id, name, preset, base_url, model, credential_ciphertext, version)
		VALUES ($1, $2::uuid, 'work', 'openai-chat', 'https://api.example.test', 'model-9', '\x00'::bytea, $3::uuid)`,
		human, connID, uuid.NewString())
	mustExec(t, local, `UPDATE model_connection_selections SET kind='api', connection_id=$2::uuid WHERE human_id=$1`,
		human, connID)
	if code, b := binding(t, local, pid); code != 200 || b.Selection != "api" {
		t.Fatalf("post-abort binding: %d %+v", code, b)
	}

	// A persona that itself arrived by transfer: pid2 carries an api
	// intent onto cloud where its bound human has no selection yet, so
	// the intent is still owed. Sealing it onward must preserve that
	// intent (no selection to snapshot), and aborting that second move
	// restores it — the persona is still gated, exactly as before.
	pid2 := newID(t)
	src2 := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, src2)
	must(drop(local.state.EnsurePersona(ctx, pid2, &src2, "configured")))
	conn2 := uuid.NewString()
	mustExec(t, local, `INSERT INTO model_api_connections
		(human_id, connection_id, name, preset, base_url, model, credential_ciphertext, version)
		VALUES ($1, $2::uuid, 'work', 'openai-chat', 'https://api.example.test', 'model-9', '\x00'::bytea, $3::uuid)`,
		src2, conn2, uuid.NewString())
	mustExec(t, local, `INSERT INTO model_connection_selections (human_id, kind, connection_id) VALUES ($1, 'api', $2::uuid)`, src2, conn2)
	must(local.svc.Seal(ctx, pid2, "move-in-2", placementID(t, cloud)))
	b2, _ := exportBytes(t, local, pid2, "move-in-2")
	dst2 := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, dst2)
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(b2), &dst2, false)))
	activated := must(cloud.svc.Activate(ctx, pid2, "move-in-2"))
	must(local.svc.Complete(ctx, pid2, "move-in-2", activated.ActivateProof))
	if code, b := binding(t, cloud, pid2); code != 200 || b.Selection != "needs_rebinding" {
		t.Fatalf("carried intent should gate on cloud: %d %+v", code, b)
	}
	carried := modelIntent(t, cloud, pid2)
	// Seal onward with no selection present: the carried intent is what
	// the persona owes, so it — not NULL — is what the next bundle carries.
	sealed2 := must(cloud.svc.Seal(ctx, pid2, "move-out-2", placementID(t, edge)))
	b3, _ := exportBytes(t, cloud, pid2, "move-out-2")
	must(drop(edge.svc.Import(ctx, bytes.NewReader(b3), nil, false)))
	var edgeIntent map[string]any
	if err := json.Unmarshal(modelIntent(t, edge, pid2), &edgeIntent); err != nil || edgeIntent["kind"] != "api" {
		t.Fatalf("the second move carried intent %s, want the unresolved api intent", modelIntent(t, edge, pid2))
	}
	retired2 := retireDest(t, edge, pid2, "move-out-2", "")
	must(cloud.svc.Abort(ctx, pid2, "move-out-2", retired2.RetireProof))
	if got := modelIntent(t, cloud, pid2); !bytes.Equal(got, carried) {
		t.Fatalf("abort restored %s, want the carried intent %s", got, carried)
	}
	var prior map[string]any
	if err := json.Unmarshal(sealed2.PriorModelIntent, &prior); err != nil || prior["kind"] != "api" {
		t.Fatalf("second seal recorded prior intent %s", sealed2.PriorModelIntent)
	}
	if code, b := binding(t, cloud, pid2); code != 200 || b.Selection != "needs_rebinding" {
		t.Fatalf("the restored intent must still gate: %d %+v", code, b)
	}
}

// The intent is part of the sealed cut — Export reads it live — so the
// clear escape is fenced to staged and active personas. On a sealed source
// it refuses; the honest path (retire → abort → clear → re-seal) works.
func TestSealedCutRefusesIntentClear(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	human := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, human)
	must(drop(local.state.EnsurePersona(ctx, pid, &human, "configured-none")))
	mustExec(t, local, `INSERT INTO model_connection_selections (human_id, kind) VALUES ($1, 'none')`, human)
	must(local.svc.Seal(ctx, pid, "move-strip", placementID(t, cloud)))

	// The store fence and the HTTP fence agree: sealed refuses.
	if err := local.state.ClearModelIntent(ctx, pid); !errors.Is(err, agentstate.ErrPersonaInactive) {
		t.Fatalf("clear on a sealed persona: %v, want inactive", err)
	}
	srv := agentstate.NewServer(local.pool, "portable-test-admin-secret")
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)
	req := httptest.NewRequest("DELETE", "/internal/core/personas/"+pid+"/model/intent", nil)
	req.Header.Set("Authorization", "Bearer portable-test-admin-secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != 409 {
		t.Fatalf("HTTP clear on a sealed persona: %d %s", rec.Code, rec.Body)
	}
	// The export still carries the intent — the bundle is not stripped.
	bundle, _ := exportBytes(t, local, pid, "move-strip")
	var prow struct {
		Data struct {
			ModelIntent map[string]any `json:"model_intent"`
		} `json:"data"`
	}
	for _, line := range bytes.Split(bundle, []byte("\n")) {
		if bytes.Contains(line, []byte(`"table":"core_personas"`)) {
			if err := json.Unmarshal(line, &prow); err != nil {
				t.Fatal(err)
			}
		}
	}
	if prow.Data.ModelIntent["kind"] != "none" {
		t.Fatalf("bundle intent after refused clear: %v", prow.Data.ModelIntent)
	}

	// The staged destination may clear it — the recovery escape is real.
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false)))
	if err := cloud.state.ClearModelIntent(ctx, pid); err != nil {
		t.Fatalf("clear on a staged destination persona: %v", err)
	}
	if got := modelIntent(t, cloud, pid); got != nil {
		t.Fatalf("staged clear left intent %s", got)
	}
	// Honest source path: retire, abort (restores the pre-seal NULL),
	// then clearing is permitted and a re-seal works.
	retired := retireDest(t, cloud, pid, "move-strip", "")
	must(local.svc.Abort(ctx, pid, "move-strip", retired.RetireProof))
	if err := local.state.ClearModelIntent(ctx, pid); err != nil {
		t.Fatalf("clear on the aborted-and-active source: %v", err)
	}
	if _, err := local.svc.Seal(ctx, pid, "move-reseal", placementID(t, cloud)); err != nil {
		t.Fatalf("re-seal after abort+clear: %v", err)
	}
	if got := modelIntent(t, local, pid); got == nil || !bytes.Contains(got, []byte(`"none"`)) {
		t.Fatalf("re-seal re-snapshotted intent %s", got)
	}
}

// Crafted incoherence in the persona row and in decision fields is refused
// transactionally; a pending approval carrying prior_* history is a legal
// re-pend artifact and is carried through.
func TestImportRejectsMalformedIntentAndDebris(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	srcHuman := newID(t)
	mustExec(t, local, `INSERT INTO humans (human_id) VALUES ($1)`, srcHuman)
	apprID := parkedApproval(t, local, pid, srcHuman)
	must(local.svc.Seal(ctx, pid, "move-shape", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-shape")

	cases := map[string]struct {
		bundle []byte
		want   string
	}{
		"intent is not an object": {
			mutateTableRows(t, bundle, "core_personas", func(d map[string]any) {
				d["model_intent"] = 42
			}), "model_intent_malformed"},
		"intent kind is not a string": {
			mutateTableRows(t, bundle, "core_personas", func(d map[string]any) {
				d["model_intent"] = map[string]any{"kind": 42}
			}), "model_intent_malformed"},
		"intent kind is unknown": {
			mutateTableRows(t, bundle, "core_personas", func(d map[string]any) {
				d["model_intent"] = map[string]any{"kind": "bogus"}
			}), "model_intent_malformed"},
		"intent lacks kind": {
			mutateTableRows(t, bundle, "core_personas", func(d map[string]any) {
				d["model_intent"] = map[string]any{"note": "x"}
			}), "model_intent_malformed"},
		"pending approval with decision debris": {
			mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
				d["decided_by_kind"] = "human"
				d["provenance"] = "agent_own_with_human_consent"
			}), "pending_approval_decided_fields"},
		"decided approval without a decider kind": {
			mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
				d["status"] = "approved"
				d["decision"] = "approve_once"
				d["decision_id"] = "d-forge"
				d["decided_by_id"] = srcHuman
				d["decided_at"] = "2026-09-14T00:00:00Z"
				d["provenance"] = "agent_own_with_human_consent"
			}), "decided_approval_incomplete"},
	}
	n := 0
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			n++
			tid := fmt.Sprintf("move-shape-%02d", n)
			_, _, err := cloud.svc.Import(ctx, bytes.NewReader(retargetTransfer(t, tc.bundle, tid)), nil, false)
			if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("import err = %v, want ErrIntegrity mentioning %q", err, tc.want)
			}
			if _, err := cloud.state.PersonaState(ctx, pid); !errors.Is(err, agentstate.ErrPersonaNotFound) {
				t.Fatalf("a refused cut left a persona behind: %v", err)
			}
		})
	}

	// Positive: a pending approval carrying prior_* decision history is
	// exactly what a cross-authority re-pend produces — legal, carried,
	// and preserved verbatim through a second move.
	legal := retargetTransfer(t, mutateTableRows(t, bundle, "core_tool_approvals", func(d map[string]any) {
		d["prior_decision"] = "approve_once"
		d["prior_decided_by_kind"] = "human"
		d["prior_decided_by_id"] = srcHuman
		d["prior_decided_at"] = "2026-09-14T00:00:00Z"
	}), "move-shape-legal")
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(legal), nil, false)))
	a := must(cloud.state.GetApproval(ctx, pid, apprID))
	if a.Status != "pending" || a.PriorDecision == nil || *a.PriorDecision != "approve_once" ||
		a.PriorDecidedByID == nil || *a.PriorDecidedByID != srcHuman {
		t.Fatalf("carried re-pend provenance %+v", a)
	}
	// Moving again while still pending keeps the provenance: bind, activate,
	// seal, export — the second bundle carries the same prior_* record.
	dstHuman := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, dstHuman)
	must(cloud.state.BindHuman(ctx, pid, dstHuman))
	must(cloud.svc.Activate(ctx, pid, "move-shape-legal"))
	must(cloud.svc.Seal(ctx, pid, "move-shape-again", placementID(t, local)))
	again, _ := exportBytes(t, cloud, pid, "move-shape-again")
	againAppr := tableRow(t, again, "core_tool_approvals", apprID)
	if againAppr["prior_decision"] != "approve_once" ||
		againAppr["prior_decided_by_id"] != srcHuman || againAppr["status"] != "pending" {
		t.Fatalf("second move lost re-pend provenance: %v", againAppr)
	}
}

// A budget-parked input is 'waiting' on placement-local backpressure — the
// funding it waits on is account state that never travels — so seal
// requeues it: carrying the wait would strand the input at the destination
// (no pending approval exists to resume it) or fail the cut's own
// verification.
func TestSealRequeuesBudgetWaitedInput(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "secretary")))
	submit(t, local, pid, "in-1", "run the numbers")
	// A cap the admission cannot fit parks the turn: the input waits.
	connID := uuid.NewString()
	must(local.state.SetBudgetAdmin(ctx, "connection", connID, agentstate.UsageBudget{
		LimitMinor: 50, Currency: "USD",
		RateInputPerMTok: 250, RateOutputPerMTok: 1000,
		PricingRevision: "test",
	}))
	gen := must(local.state.AcquireWriter(ctx, pid, "local-core", time.Minute)).Generation
	must(local.state.Recover(ctx, pid, gen))
	load := must(local.state.LoadTurn(ctx, pid, gen, "turn-1", 50))
	if load.Input == nil || load.Input.InputID != "in-1" {
		t.Fatalf("turn-1 claimed %+v", load.Input)
	}
	if _, err := local.state.CommitTurn(ctx, pid, "turn-1", gen, agentstate.CommitRequest{
		Outcome: "await",
		Wait: &agentstate.CommitWait{
			Kind: "budget", NeededMinor: 100, Currency: "USD",
			Funding: agentstate.FundingRef{Kind: "connection", ID: connID},
		},
	}); err != nil {
		t.Fatalf("budget park: %v", err)
	}
	var status string
	if err := local.pool.QueryRow(ctx,
		`SELECT status FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-1'`, pid).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "waiting" {
		t.Fatalf("parked input status %s, want waiting", status)
	}

	// The seal must not fail the cut on the wait, and the carried input
	// must arrive resumable — 'queued', not stranded on a funding row the
	// destination does not have.
	if _, err := local.svc.Seal(ctx, pid, "move-wait", placementID(t, cloud)); err != nil {
		t.Fatalf("seal with a budget wait: %v", err)
	}
	if err := local.pool.QueryRow(ctx,
		`SELECT status FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-1'`, pid).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("sealed input status %s, want queued", status)
	}
	var waits int
	if err := local.pool.QueryRow(ctx,
		`SELECT count(*) FROM core_budget_waits WHERE persona_id = $1`, pid).Scan(&waits); err != nil {
		t.Fatal(err)
	}
	if waits != 0 {
		t.Fatalf("%d budget waits survived the seal", waits)
	}

	bundle, _ := exportBytes(t, local, pid, "move-wait")
	if bytes.Contains(bundle, []byte("core_budget_waits")) {
		t.Fatal("budget wait rows traveled in the bundle")
	}
	dstHuman := newID(t)
	mustExec(t, cloud, `INSERT INTO humans (human_id) VALUES ($1)`, dstHuman)
	must(drop(cloud.svc.Import(ctx, bytes.NewReader(bundle), &dstHuman, false)))
	must(cloud.svc.Activate(ctx, pid, "move-wait"))
	// The destination's writer claims the requeued input under its own
	// funding — a fresh denial there parks it again; it is never stranded.
	dgen := must(cloud.state.AcquireWriter(ctx, pid, "cloud-core", time.Minute)).Generation
	must(cloud.state.Recover(ctx, pid, dgen))
	dload := must(cloud.state.LoadTurn(ctx, pid, dgen, "d-turn-1", 50))
	if dload.Input == nil || dload.Input.InputID != "in-1" {
		t.Fatalf("destination claimed %+v, want in-1", dload.Input)
	}
}
