package portable

// Carried input_received integrity (the G3 gap): a journal receipt must
// name, as a JSON string, a real input of this same secretary, and the
// input's received_seq must point back at that receipt. The write boundary
// already refuses to journal anything else, so the only shapes that reach
// verifyCut are crafted ones — a corrupted source DB or a digest-recomputed
// bundle. Both run through the same check set here, on the real store.
//
// Every discriminating case gets its own destination placement (fresh
// persona space and empty transfer ledger), so a regression that admits one
// crafted case cannot mask the next behind ErrPersonaExists or a
// ledger conflict — each subcase exercises its own condition.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
)

// bundleEdit describes one crafted re-issue of an exported bundle.
type bundleEdit struct {
	transferID    string              // new transfer id (required)
	destinationID string              // re-address to this placement (optional)
	bumpEventCut  int64               // add to cut.latest_event_seq
	mutateRow     func(string) string // applied to every row line (mutRow output)
	appendTable   string              // table the appended row belongs to
	appendData    map[string]any      // row appended after the table's last row
}

// editBundle rebuilds an exported bundle per bundleEdit and recomputes the
// trailer's row counts and content digest — a structurally valid bundle
// carrying state no honest export writes, which is what verifyCut refuses.
func editBundle(t *testing.T, bundle []byte, e bundleEdit) []byte {
	t.Helper()
	var lines []string
	sc := bufio.NewScanner(bytes.NewReader(bundle))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		lines = append(lines, sc.Text()+"\n")
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	h := sha256.New()
	write := func(line string) {
		h.Write([]byte(line))
		out.WriteString(line)
	}
	appended := false
	for i, line := range lines {
		switch {
		case i == 0:
			var hdr Header
			if err := strictDecode([]byte(line), &hdr); err != nil {
				t.Fatalf("header: %v", err)
			}
			hdr.TransferID = e.transferID
			if e.destinationID != "" {
				hdr.DestinationID = e.destinationID
			}
			hdr.Cut.LatestEventSeq += e.bumpEventCut
			raw, _ := json.Marshal(hdr)
			write(string(raw) + "\n")
		case strings.Contains(line, `"record":"trailer"`):
			var tr Trailer
			if err := json.Unmarshal([]byte(line), &tr); err != nil {
				t.Fatalf("trailer: %v", err)
			}
			if e.appendData != nil {
				tr.Rows[e.appendTable]++
			}
			tr.ContentSHA256 = hex.EncodeToString(h.Sum(nil))
			raw, _ := json.Marshal(tr)
			out.Write(append(raw, '\n'))
		default:
			if e.mutateRow != nil {
				line = e.mutateRow(line)
			}
			write(line)
			// The appended row keeps required table ordering by landing
			// directly after its table's last row.
			if e.appendData != nil && !appended &&
				strings.Contains(line, `"table":"`+e.appendTable+`"`) &&
				!strings.Contains(lines[i+1], `"table":"`+e.appendTable+`"`) {
				rec, _ := json.Marshal(rowRecord{Record: "row", Section: CoreSection,
					Table: e.appendTable, Data: must(json.Marshal(e.appendData))})
				write(string(rec) + "\n")
				appended = true
			}
		}
	}
	if e.appendData != nil && !appended {
		t.Fatalf("bundle has no %s rows to append after", e.appendTable)
	}
	return out.Bytes()
}

// ghostEvent is a fabricated core_events row: a journal receipt for an
// input arrival that never happened. It borrows a real turn_id (the
// event-turn check would otherwise catch it first) and the next journal
// seq (the contiguity check).
func ghostEvent(t *testing.T, bundle []byte, inputID any) map[string]any {
	t.Helper()
	hdr := bundleHeader(t, bundle)
	var turnID string
	var maxSeq int64
	sc := bufio.NewScanner(bytes.NewReader(bundle))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		line := sc.Text()
		if !strings.Contains(line, `"table":"core_events"`) {
			continue
		}
		var rec rowRecord
		if err := strictDecode([]byte(line), &rec); err != nil {
			t.Fatalf("event row: %v", err)
		}
		var d map[string]any
		if err := json.Unmarshal(rec.Data, &d); err != nil {
			t.Fatalf("event data: %v", err)
		}
		turnID, _ = d["turn_id"].(string)
		if s, ok := d["seq"].(float64); ok && int64(s) > maxSeq {
			maxSeq = int64(s)
		}
	}
	return map[string]any{
		"persona_id": hdr.PersonaID,
		"seq":        maxSeq + 1,
		"turn_id":    turnID,
		"kind":       "input_received",
		"payload":    map[string]any{"input_id": inputID},
		"created_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// integrityFail asserts err is ErrIntegrity naming want among its checks.
func integrityFail(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want ErrIntegrity mentioning %q, got nil", want)
	}
	if !errors.Is(err, ErrIntegrity) || !strings.Contains(err.Error(), want) {
		t.Fatalf("err = %v, want ErrIntegrity mentioning %q", err, want)
	}
}

// refusedImportProvesNoResidue asserts a refused Import left nothing
// activatable behind: no persona, no staged rows, no transfer ledger entry.
func refusedImportProvesNoResidue(t *testing.T, p placement, personaID, transferID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := p.state.PersonaState(ctx, personaID); !errors.Is(err, agentstate.ErrPersonaNotFound) {
		t.Fatalf("refused import left a persona behind: %v", err)
	}
	var rows int64
	if err := p.pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM core_events WHERE persona_id = $1)
			+ (SELECT count(*) FROM core_inputs WHERE persona_id = $1)`,
		personaID).Scan(&rows); err != nil {
		t.Fatalf("count residue: %v", err)
	}
	if rows != 0 {
		t.Fatalf("refused import left %d staged rows", rows)
	}
	if _, err := p.svc.Status(ctx, "import", transferID); !errors.Is(err, ErrTransferNotFound) {
		t.Fatalf("refused import left ledger state: %v", err)
	}
}

// A receipt naming an input this persona never received — the G3 ghost —
// must refuse at import; before the fix the inner join dropped the orphan
// row and the bundle staged and activated.
func TestImportRejectsReceiptForAbsentInput(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)

	must(local.svc.Seal(ctx, pid, "move-ghost", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-ghost")
	ghost := editBundle(t, bundle, bundleEdit{
		transferID:   "move-ghost2",
		bumpEventCut: 1,
		appendTable:  "core_events",
		appendData:   ghostEvent(t, bundle, "input-that-never-arrived"),
	})

	_, _, err := cloud.svc.Import(ctx, bytes.NewReader(ghost), nil, false)
	integrityFail(t, err, "input_received_input_missing")
	refusedImportProvesNoResidue(t, cloud, pid, "move-ghost2")

	// The refusal is not a tombstone: the legitimate bundle still stages
	// under its own transfer id and activates — and the queued input with
	// no journaled receipt (received_seq NULL) crosses untouched.
	if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); err != nil || !created {
		t.Fatalf("legit import after refusal: created=%v err=%v", created, err)
	}
	must(cloud.svc.Activate(ctx, pid, "move-ghost"))
	var status string
	var rseq *int64
	if err := cloud.pool.QueryRow(ctx,
		`SELECT status, received_seq FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-3'`,
		pid).Scan(&status, &rseq); err != nil {
		t.Fatalf("carried queued input: %v", err)
	}
	if status != "queued" || rseq != nil {
		t.Fatalf("queued input carried as status=%s received_seq=%v", status, rseq)
	}
}

// A receipt naming an input that exists only on another persona is equally
// absent — on BOTH sides. The destination placement holds its own different
// persona owning the same input id, so an unscoped NOT EXISTS (dropping
// i.persona_id = e.persona_id) would resolve the reference and stage: this
// fixture discriminates persona scoping itself, not only the join shape.
func TestImportRejectsReceiptForOtherPersonasInput(t *testing.T) {
	ctx := context.Background()
	local, cloud := newPlacement(t), newPlacement(t)
	pid, other := newID(t), newID(t)
	liveSecretary(t, local, pid)
	must(drop(local.state.EnsurePersona(ctx, other, nil, "other secretary")))
	submit(t, local, other, "shared-input-id", "another persona's input")
	// The colliding input id also exists on the destination, owned by a
	// different persona there — verifyCut runs against this database.
	destOther := newID(t)
	must(drop(cloud.state.EnsurePersona(ctx, destOther, nil, "destination other")))
	submit(t, cloud, destOther, "shared-input-id", "destination persona's input")

	must(local.svc.Seal(ctx, pid, "move-xpers", placementID(t, cloud)))
	bundle, _ := exportBytes(t, local, pid, "move-xpers")
	ghost := editBundle(t, bundle, bundleEdit{
		transferID:   "move-xpers2",
		bumpEventCut: 1,
		appendTable:  "core_events",
		appendData:   ghostEvent(t, bundle, "shared-input-id"),
	})

	_, _, err := cloud.svc.Import(ctx, bytes.NewReader(ghost), nil, false)
	integrityFail(t, err, "input_received_input_missing")
	refusedImportProvesNoResidue(t, cloud, pid, "move-xpers2")
}

// claimAndCommitInput runs one complete turn claiming inputID, so its real
// input_received receipt is journaled and received_seq links to it.
func claimAndCommitInput(t *testing.T, p placement, personaID, inputID string) {
	t.Helper()
	ctx := context.Background()
	submit(t, p, personaID, inputID, "digits-only id")
	gen := must(p.state.AcquireWriter(ctx, personaID, "local-core", time.Minute)).Generation
	load := must(p.state.LoadTurn(ctx, personaID, gen, "t-1", 50))
	if load.Input == nil || load.Input.InputID != inputID {
		t.Fatalf("turn claimed %+v", load.Input)
	}
	must(p.state.CommitTurn(ctx, personaID, "t-1", gen, agentstate.CommitRequest{
		Outcome: "complete",
		Events: []agentstate.EventInput{{Kind: "input_received", Payload: map[string]any{
			"input_id": inputID, "kind": "message", "text": "digits-only id",
			"actor_kind": "human", "source_surface": "test", "attempt": 1,
		}}},
	}))
	var linked bool
	if err := p.pool.QueryRow(ctx, `
		SELECT received_seq IS NOT NULL FROM core_inputs
		WHERE persona_id = $1 AND input_id = $2`, personaID, inputID).Scan(&linked); err != nil || !linked {
		t.Fatalf("claimed input receipt link: %v %v", linked, err)
	}
}

// A receipt whose payload.input_id is not a JSON string is malformed even
// when the value stringifies onto a real text input id: ->> hides the type
// difference, so only an explicit type check can catch a numeric 4242
// aliasing input "4242" whose real received_seq points at that very event.
// Each case gets a fresh destination so one admitted case cannot mask the
// next behind ErrPersonaExists or a transfer-ledger conflict.
func TestImportRejectsNonStringReceiptID(t *testing.T) {
	ctx := context.Background()
	local := newPlacement(t)
	pid := newID(t)
	must(drop(local.state.EnsurePersona(ctx, pid, nil, "secretary")))
	claimAndCommitInput(t, local, pid, "4242")

	bootstrap := newPlacement(t)
	must(local.svc.Seal(ctx, pid, "move-num", placementID(t, bootstrap)))
	bundle, _ := exportBytes(t, local, pid, "move-num")

	eventRow := func(d map[string]any) bool { return d["kind"] == "input_received" }
	cases := map[string]struct {
		tid     string
		payload map[string]any
	}{
		// Scalar/text collision: JSON 4242 stringifies to the real text id
		// "4242" — every value join resolves; only the type check refuses.
		"numeric id colliding with text id": {"move-num-n", map[string]any{"input_id": 4242.0}},
		"boolean id":                        {"move-num-b", map[string]any{"input_id": true}},
		"object id":                         {"move-num-o", map[string]any{"input_id": map[string]any{"id": "4242"}}},
		"array id":                          {"move-num-a", map[string]any{"input_id": []any{"4242"}}},
		// JSON null and a missing key are distinct supported shapes: the
		// first yields jsonb_typeof 'null', the second SQL NULL.
		"json null":   {"move-num-j", map[string]any{"input_id": nil}},
		"missing key": {"move-num-m", map[string]any{"kind": "message"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cloud := newPlacement(t)
			bad := editBundle(t, bundle, bundleEdit{
				transferID:    tc.tid,
				destinationID: placementID(t, cloud),
				mutateRow: mutRow(t, "core_events", eventRow, func(d map[string]any) {
					d["payload"] = tc.payload
				}),
			})
			_, _, err := cloud.svc.Import(ctx, bytes.NewReader(bad), nil, false)
			integrityFail(t, err, "input_received_input_id_not_string")
			refusedImportProvesNoResidue(t, cloud, pid, tc.tid)
		})
	}
}

// The reverse link, exercised the other way: a receipt naming a real input
// whose received_seq does not point back at it is still a broken carried
// reference — an unlinked receipt and one double-claiming an
// already-linked input both refuse. Fresh destination per case, so a
// regression admitting one cannot shadow the other.
func TestImportRejectsUnlinkedReceipts(t *testing.T) {
	ctx := context.Background()
	local := newPlacement(t)
	pid := newID(t)
	liveSecretary(t, local, pid)

	bootstrap := newPlacement(t)
	must(local.svc.Seal(ctx, pid, "move-unlink", placementID(t, bootstrap)))
	bundle, _ := exportBytes(t, local, pid, "move-unlink")

	for name, tc := range map[string]struct {
		transferID string
		ghostID    string
	}{
		// in-3 is queued and never journaled: a receipt for it claims an
		// arrival record that does not exist yet.
		"receipt for queued input": {"move-unlink-q", "in-3"},
		// in-1's real receipt already holds the link; a second receipt for
		// it is a duplicate arrival record.
		"receipt double-claiming a linked input": {"move-unlink-d", "in-1"},
	} {
		t.Run(name, func(t *testing.T) {
			cloud := newPlacement(t)
			ghost := editBundle(t, bundle, bundleEdit{
				transferID:    tc.transferID,
				destinationID: placementID(t, cloud),
				bumpEventCut:  1,
				appendTable:   "core_events",
				appendData:    ghostEvent(t, bundle, tc.ghostID),
			})
			_, _, err := cloud.svc.Import(ctx, bytes.NewReader(ghost), nil, false)
			integrityFail(t, err, "input_received_seq_not_linked")
			refusedImportProvesNoResidue(t, cloud, pid, tc.transferID)
		})
	}
}

// The same checks run on the source at seal: a corrupted live journal must
// refuse, and the refusal must roll the whole seal transaction back — the
// source stays active with its writer generation unburned and no export
// ledger row, so it recovers instead of being left half-sealed. Each case
// gets a fresh source persona so an admitted seal cannot leave later cases
// an already-sealed authority.
func TestSealRejectsCorruptedSourceReceipts(t *testing.T) {
	ctx := context.Background()

	ghostSQL := `
		INSERT INTO core_events (persona_id, seq, turn_id, kind, payload, created_at)
		VALUES ($1::uuidv7,
			(SELECT COALESCE(MAX(seq),0)+1 FROM core_events WHERE persona_id = $1),
			(SELECT turn_id FROM core_events WHERE persona_id = $1 LIMIT 1),
			'input_received', $2::jsonb, now())`
	cases := map[string]struct {
		payload    string
		otherInput bool // create a second persona holding the named input
		want       string
	}{
		// The write boundary refuses this, so a ghost in the live DB means
		// row-level corruption — seal must catch what the boundary missed.
		"receipt for absent input": {`{"input_id":"no-such-input"}`, false, "input_received_input_missing"},
		// The id exists but only on another persona's inputs; the
		// persona-scoped lookup must not resolve it.
		"receipt for another persona's input": {`{"input_id":"other-pid-input"}`, true, "input_received_input_missing"},
		// A receipt missing its input_id key entirely.
		"receipt without input_id": {`{"kind":"message"}`, false, "input_received_input_id_not_string"},
		// A receipt whose input_id is explicit JSON null.
		"receipt with json null input_id": {`{"input_id":null}`, false, "input_received_input_id_not_string"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			local, cloud := newPlacement(t), newPlacement(t)
			pid := newID(t)
			liveSecretary(t, local, pid)
			if tc.otherInput {
				other := newID(t)
				must(drop(local.state.EnsurePersona(ctx, other, nil, "other secretary")))
				submit(t, local, other, "other-pid-input", "another persona's input")
			}
			genBefore := must(local.state.PersonaState(ctx, pid)).Lease.Generation

			mustExec(t, local, ghostSQL, pid, tc.payload)
			_, err := local.svc.Seal(ctx, pid, "move-corrupt", placementID(t, cloud))
			integrityFail(t, err, tc.want)

			st := must(local.state.PersonaState(ctx, pid))
			if st.Persona.Authority != "active" {
				t.Fatalf("authority after refused seal: %q, want active", st.Persona.Authority)
			}
			if st.Lease == nil || st.Lease.Generation != genBefore {
				t.Fatalf("refused seal burned the writer generation: %+v, want %d", st.Lease, genBefore)
			}
			if _, err := local.svc.Status(ctx, "export", "move-corrupt"); !errors.Is(err, ErrTransferNotFound) {
				t.Fatalf("refused seal left a ledger row: %v", err)
			}

			// Removing the planted row restores a fully transferable
			// source: the refusal was recoverable, not a broken persona.
			mustExec(t, local, `DELETE FROM core_events WHERE persona_id = $1
				AND seq = (SELECT max(seq) FROM core_events WHERE persona_id = $1)`, pid)
			must(local.svc.Seal(ctx, pid, "move-recovered", placementID(t, cloud)))
			bundle, _ := exportBytes(t, local, pid, "move-recovered")
			if _, created, err := cloud.svc.Import(ctx, bytes.NewReader(bundle), nil, false); err != nil || !created {
				t.Fatalf("import after recovered seal: created=%v err=%v", created, err)
			}
		})
	}

	// A numeric receipt aliasing a real text id, on a persona whose only
	// receipt is mutated in place to a JSON number: every value join still
	// resolves — received_seq, seq equality, the row itself — so only the
	// JSON-type check refuses.
	t.Run("numeric receipt aliasing real text id", func(t *testing.T) {
		local, cloud := newPlacement(t), newPlacement(t)
		pid := newID(t)
		must(drop(local.state.EnsurePersona(ctx, pid, nil, "digits secretary")))
		claimAndCommitInput(t, local, pid, "9000")
		mustExec(t, local, `
			UPDATE core_events SET payload = '{"input_id":9000}'::jsonb
			WHERE persona_id = $1 AND kind = 'input_received'`, pid)
		_, err := local.svc.Seal(ctx, pid, "move-corrupt-num", placementID(t, cloud))
		integrityFail(t, err, "input_received_input_id_not_string")
		st := must(local.state.PersonaState(ctx, pid))
		if st.Persona.Authority != "active" {
			t.Fatalf("authority after refused seal: %q, want active", st.Persona.Authority)
		}
	})
}
