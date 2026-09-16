package transfersession_test

// Repair tests for F234 (one move URL, two Sumi Local installs) and the
// F233 admission race, at the service and route level. The Local command's
// side of the same repair is in cmd/local-move/duplicate_url_test.go.

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/transfersession"
)

// otherLocal is a second, real Local placement with its own database,
// placement id and secretary.
func otherLocal(t *testing.T, h *harness, name string) (placement, transfersession.Source) {
	t.Helper()
	p := newPlacement(t, true)
	pid := newID(t)
	if _, _, err := p.state.EnsurePersona(h.ctx, pid, nil, name); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.state.SubmitInput(h.ctx, &agentstate.Input{
		PersonaID: pid, InputID: "in-other", Kind: "message",
		Payload: map[string]any{"text": "こちらの秘書"}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
	}); err != nil {
		t.Fatal(err)
	}
	place, err := p.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return p, transfersession.Source{PlacementID: place, PersonaID: pid}
}

func (h *harness) ownSource(t *testing.T) transfersession.Source {
	t.Helper()
	own, err := h.local.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return transfersession.Source{PlacementID: own, PersonaID: h.pid}
}

// Taking the move URL is decided once, under the session row lock. Racing
// placements produce exactly one holder — never two seals — and the holder's
// own retries all succeed.
func TestOneMoveURLHasOneSource(t *testing.T) {
	h := setup(t, transfersession.Config{})
	sid, grant := h.create(uidFor(t, "onesource"))

	const n = 8
	sources := make([]transfersession.Source, n)
	sources[0] = h.ownSource(t)
	for i := 1; i < n; i++ {
		sources[i] = transfersession.Source{PlacementID: newID(t), PersonaID: newID(t)}
	}
	codes := make([]int, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range sources {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			code, _ := h.bindAs(sid, grant, sources[i])
			codes[i] = code
		}(i)
	}
	close(start)
	wg.Wait()
	ok := 0
	for i, c := range codes {
		switch c {
		case http.StatusOK:
			ok++
		case http.StatusConflict:
		default:
			t.Fatalf("source %d: %d", i, c)
		}
	}
	if ok != 1 {
		t.Fatalf("%d placements took one move URL", ok)
	}
	var place, persona string
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT source_placement_id, source_persona_id FROM transfer_sessions WHERE session_id = $1`, sid).
		Scan(&place, &persona); err != nil {
		t.Fatal(err)
	}
	holder := transfersession.Source{PlacementID: place, PersonaID: persona}
	// The holder repeats itself as often as it likes: lost answers, retries
	// and restarts all reach the same row.
	for i := 0; i < 3; i++ {
		if code, v := h.bindAs(sid, grant, holder); code != http.StatusOK || v.Source == nil || *v.Source != holder {
			t.Fatalf("holder re-bind %d: %d %+v", i, code, v.Source)
		}
	}
	// Everyone else keeps being refused, and is told who holds it.
	code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/source", grant,
		jsonBody(transfersession.Source{PlacementID: newID(t), PersonaID: newID(t)}))
	if code != http.StatusConflict || !strings.Contains(string(raw), holder.PersonaID) {
		t.Fatalf("late placement: %d %s", code, raw)
	}
}

// A bundle is admitted only for the placement that took the URL, at every
// status, and a placement that never took it cannot upload at all.
func TestOnlyTheBoundSourceMayUpload(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := uidFor(t, "boundupload")
	sid, grant := h.create(uid)
	dest, err := h.cloud.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Nobody took the URL yet: a bundle sealed out of protocol is refused
	// before anything is imported.
	other, otherSrc := otherLocal(t, h, "second install")
	if _, err := other.svc.Seal(h.ctx, otherSrc.PersonaID, sid, dest); err != nil {
		t.Fatal(err)
	}
	var bufB bytes.Buffer
	if _, err := other.svc.Export(h.ctx, otherSrc.PersonaID, sid, &bufB); err != nil {
		t.Fatal(err)
	}
	code, raw := h.request("PUT", "/api/secretary-transfer/sessions/"+sid+"/bundle", grant, bytes.NewReader(bufB.Bytes()))
	if code != http.StatusConflict || !strings.Contains(string(raw), "no Sumi Local placement has taken this move URL") {
		t.Fatalf("unbound upload: %d %s", code, raw)
	}
	if rows := h.importRows(sid); rows != 0 {
		t.Fatalf("an unbound upload was imported: %d rows", rows)
	}

	// The real source takes the URL, seals and stages.
	bundle := h.seal(sid, grant)
	if code, v := h.upload(sid, grant, bytes.NewReader(bundle)); code != http.StatusCreated || v.Status != transfersession.StatusStaged {
		t.Fatalf("bound upload: %d %+v", code, v)
	}
	// The other install's bundle is refused after the winner staged too, and
	// the answer names the secretary this session accepts instead of handing
	// back the winner's view.
	code, raw = h.request("PUT", "/api/secretary-transfer/sessions/"+sid+"/bundle", grant, bytes.NewReader(bufB.Bytes()))
	if code != http.StatusConflict || !strings.Contains(string(raw), "accepts secretary "+h.pid) {
		t.Fatalf("staged-session upload from another placement: %d %s", code, raw)
	}
	// The bound source's own repeat still answers the current view.
	if code, v := h.upload(sid, grant, bytes.NewReader(bundle)); code != http.StatusOK || v.Status != transfersession.StatusStaged {
		t.Fatalf("bound source repeat: %d %+v", code, v)
	}
}

// A cancel that presents another placement's persona and key cannot write the
// tombstone that would foreclose the bound source's transfer.
func TestForeignCancelCannotForecloseTheBoundTransfer(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := uidFor(t, "foreigncancel")
	sid, grant := h.create(uid)
	dest, err := h.cloud.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if code, v := h.bind(sid, grant); code != http.StatusOK {
		t.Fatalf("bind: %d %+v", code, v)
	}
	other, otherSrc := otherLocal(t, h, "second install")
	if _, err := other.svc.Seal(h.ctx, otherSrc.PersonaID, sid, dest); err != nil {
		t.Fatal(err)
	}
	var bufB bytes.Buffer
	if _, err := other.svc.Export(h.ctx, otherSrc.PersonaID, sid, &bufB); err != nil {
		t.Fatal(err)
	}
	hdrB := header(t, bufB.Bytes())

	code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", grant,
		jsonBody(map[string]string{"persona_id": otherSrc.PersonaID, "transfer_key": hdrB.TransferKey}))
	if code != http.StatusConflict || !strings.Contains(string(raw), "accepts secretary "+h.pid) {
		t.Fatalf("foreign cancel: %d %s", code, raw)
	}
	if got := h.dbStatus(sid); got != transfersession.StatusAwaitingBundle {
		t.Fatalf("session after the refused cancel: %s", got)
	}
	if _, err := h.cloud.svc.Status(h.ctx, "import", sid); !errors.Is(err, portable.ErrTransferNotFound) {
		t.Fatalf("a tombstone was written for another placement's key: %v", err)
	}
	// The bound source still moves normally.
	bundle := h.seal(sid, grant)
	if code, v := h.upload(sid, grant, bytes.NewReader(bundle)); code != http.StatusCreated || v.Status != transfersession.StatusStaged {
		t.Fatalf("bound upload after the refused cancel: %d %+v", code, v)
	}
}

// Taking the URL is refused once the session is closed or already carries a
// secretary, and the refusal says which.
func TestBindRefusedOnClosedAndArrivedSessions(t *testing.T) {
	t.Run("cancelled", func(t *testing.T) {
		h := setup(t, transfersession.Config{})
		uid := uidFor(t, "bindcancelled")
		sid, grant := h.create(uid)
		if code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "",
			jsonBody(h.flow(uid, time.Minute))); code != http.StatusOK {
			t.Fatalf("cancel: %d %s", code, raw)
		}
		if code, _ := h.bind(sid, grant); code != http.StatusGone {
			t.Fatalf("bind on a cancelled session: %d", code)
		}
		if got := authority(t, h.local, h.pid); got != "active" {
			t.Fatalf("local authority: %s", got)
		}
	})
	t.Run("admission deadline passed", func(t *testing.T) {
		h := setup(t, transfersession.Config{AdmitTTL: 60 * time.Millisecond})
		sid, grant := h.create(uidFor(t, "bindexpired"))
		time.Sleep(150 * time.Millisecond)
		if code, _ := h.bind(sid, grant); code != http.StatusGone {
			t.Fatalf("bind after the admission deadline: %d", code)
		}
	})
	t.Run("already staged", func(t *testing.T) {
		h := setup(t, transfersession.Config{})
		uid := uidFor(t, "bindstaged")
		sid, grant, _ := h.stage(uid)
		// The holder is still the holder.
		if code, v := h.bind(sid, grant); code != http.StatusOK || v.Status != transfersession.StatusStaged {
			t.Fatalf("holder bind on a staged session: %d %+v", code, v)
		}
		// Anyone else is refused.
		_, otherSrc := otherLocal(t, h, "second install")
		if code, _ := h.bindAs(sid, grant, otherSrc); code != http.StatusConflict {
			t.Fatalf("other bind on a staged session: %d", code)
		}
	})
}

// The ordinary recovery for the refused install: the registrant replaces the
// move URL, and the second placement takes the new one.
func TestRefusedPlacementRecoversWithANewMoveURL(t *testing.T) {
	h := setup(t, transfersession.Config{})
	uid := uidFor(t, "newurl")
	sid, grant := h.create(uid)
	if code, _ := h.bind(sid, grant); code != http.StatusOK {
		t.Fatal("first bind")
	}
	other, otherSrc := otherLocal(t, h, "second install")
	if code, _ := h.bindAs(sid, grant, otherSrc); code != http.StatusConflict {
		t.Fatal("second bind should be refused")
	}

	// The person chooses a new URL for the other install.
	code, raw := h.request("POST", "/api/secretary-transfer/sessions/"+sid+"/cancel", "",
		jsonBody(withFlow(h.flow(uid, time.Minute), map[string]string{"expect_status": "awaiting_bundle"})))
	if code != http.StatusOK {
		t.Fatalf("guarded cancel: %d %s", code, raw)
	}
	sid2, grant2 := h.create(uid)
	if code, v := h.bindAs(sid2, grant2, otherSrc); code != http.StatusOK || v.Source == nil || v.Source.PersonaID != otherSrc.PersonaID {
		t.Fatalf("second install takes the new URL: %d %+v", code, v)
	}
	dest, err := h.cloud.svc.PlacementID(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.svc.Seal(h.ctx, otherSrc.PersonaID, sid2, dest); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := other.svc.Export(h.ctx, otherSrc.PersonaID, sid2, &buf); err != nil {
		t.Fatal(err)
	}
	if code, v := h.upload(sid2, grant2, bytes.NewReader(buf.Bytes())); code != http.StatusCreated || v.Status != transfersession.StatusStaged {
		t.Fatalf("second install upload: %d %+v", code, v)
	}
	if _, _, err := h.provision(uid, sid2, nil); err != nil {
		t.Fatalf("provision the second install: %v", err)
	}
	gv, err := h.sessions.Status(h.ctx, sid2, grant2)
	if err != nil || gv.Status != transfersession.StatusActivated {
		t.Fatalf("activated: %+v %v", gv, err)
	}
	if _, err := other.svc.Complete(h.ctx, otherSrc.PersonaID, sid2, gv.ActivateProof); err != nil {
		t.Fatalf("second install complete: %v", err)
	}
	// The first install never sealed and is still answering.
	if got := authority(t, h.local, h.pid); got != "active" {
		t.Fatalf("first install authority: %s", got)
	}
}
