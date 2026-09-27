package returnsession_test

// Concurrency proofs for the durable capture-binding protocol
// (CAPINT-01..03): every page/read is preconditioned on the capture the
// caller planned, retake retries are idempotent across lost responses,
// Ensure distinguishes a genuinely-gone capture from an outage, and a
// delayed release can never clear a replacement binding or the durable
// scope anchor.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
)

// fakeCaps is a CaptureStore stand-in: sequential captures over one
// scope identity, a liveness map for gone-vs-alive answers, an injected
// GetCapture fault for the outage case, and counters so a test can
// prove no redundant mint/release churn happened.
type fakeCaps struct {
	mu       sync.Mutex
	n        int
	scopeID  string
	sha      string
	live     map[string]bool
	creates  int
	expected []string // expected_scope_id seen on each create
	released []string
	getErr   error
}

func newFakeCaps() *fakeCaps {
	return &fakeCaps{scopeID: "si-fixture", sha: "sha-fixture", live: map[string]bool{}}
}

func (f *fakeCaps) CreateCapture(_ context.Context, scope, owner string, epoch int64, expectedScopeID string) (*fileaccess.CaptureMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n++
	f.creates++
	f.expected = append(f.expected, expectedScopeID)
	if expectedScopeID != "" && expectedScopeID != f.scopeID {
		return nil, &fileaccess.ServiceError{Status: 422, Code: "scope_changed", Message: "scope anchor does not match expected scope identity"}
	}
	id := fmt.Sprintf("cap-fake-%d", f.n)
	f.live[id] = true
	return &fileaccess.CaptureMeta{
		CaptureID: id, Scope: scope, ScopeID: f.scopeID, Owner: owner,
		OwnerEpoch: epoch, ManifestSHA: f.sha, Status: "active",
	}, nil
}

func (f *fakeCaps) GetCapture(_ context.Context, id, owner string, epoch int64) (*fileaccess.CaptureMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	if !f.live[id] {
		return nil, &fileaccess.ServiceError{Status: http.StatusGone, Code: "capture_gone", Message: "capture is gone"}
	}
	return &fileaccess.CaptureMeta{
		CaptureID: id, ScopeID: f.scopeID, Owner: owner,
		OwnerEpoch: epoch, ManifestSHA: f.sha, Status: "active",
	}, nil
}

func (f *fakeCaps) ReleaseCapture(_ context.Context, id, owner string, epoch int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.live, id)
	f.released = append(f.released, id)
	return nil
}

func (f *fakeCaps) kill(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.live, id)
}

func (f *fakeCaps) counts() (creates, releases int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.creates, len(f.released)
}

// sealedLocal drives a local-mode session to sealed with the capture
// fixtures wired: the freeze fence is a fake, the capture store is
// fakeCaps. Returns the session id and grant.
func sealedLocal(t *testing.T, h *harness, caps *fakeCaps) (sessionID, grant string) {
	t.Helper()
	h.sessions.SetFileStore(&fakeFiles{})
	h.sessions.SetCaptureStore(caps)
	sid, _, g := h.createMode("local")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sid),
		g, jsonBody(destMode(t, h.local, h.persona, "absent", "local")))
	if code != http.StatusOK {
		t.Fatalf("local bind: %d %s", code, raw)
	}
	if v := asView(t, raw); v.Status != returnsession.StatusSealed {
		t.Fatalf("after bind the session is %s", v.Status)
	}
	return sid, g
}

// bindingRow reads the persisted capture columns straight from PG.
func bindingRow(t *testing.T, h *harness, sessionID string) (anchor, captureID, manifestSHA string) {
	t.Helper()
	var a, c, m *string
	if err := h.cloud.pool.QueryRow(h.ctx, `SELECT capture_scope_id, capture_id, capture_manifest_sha
		FROM return_sessions WHERE session_id = $1`, sessionID).Scan(&a, &c, &m); err != nil {
		t.Fatal(err)
	}
	if a != nil {
		anchor = *a
	}
	if c != nil {
		captureID = *c
	}
	if m != nil {
		manifestSHA = *m
	}
	return
}

// CAPINT-02: a transport fault, 5xx or denial on the GetCapture probe
// says nothing about the capture's life — Ensure must propagate it and
// keep the durable association, not mint a replacement over a healthy
// binding.
func TestEnsureCaptureTransientLookupKeepsBinding(t *testing.T) {
	h := setup(t, returnsession.Config{})
	caps := newFakeCaps()
	sid, grant := sealedLocal(t, h, caps)

	b1, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("first ensure: %v", err)
	}

	// Transport-class failure (no ServiceError at all).
	caps.mu.Lock()
	caps.getErr = errors.New("dial tcp: connection refused")
	caps.mu.Unlock()
	if _, err := h.sessions.EnsureCapture(h.ctx, sid, grant); err == nil {
		t.Fatal("ensure swallowed a transport failure")
	}
	// A 5xx is also not proof of expiry.
	caps.mu.Lock()
	caps.getErr = &fileaccess.ServiceError{Status: http.StatusBadGateway, Code: "upstream", Message: "proxy"}
	caps.mu.Unlock()
	if _, err := h.sessions.EnsureCapture(h.ctx, sid, grant); err == nil {
		t.Fatal("ensure swallowed a 5xx")
	}
	// Neither is a denial.
	caps.mu.Lock()
	caps.getErr = &fileaccess.ServiceError{Status: http.StatusForbidden, Code: "denied", Message: "barrier mismatch"}
	caps.mu.Unlock()
	if _, err := h.sessions.EnsureCapture(h.ctx, sid, grant); err == nil {
		t.Fatal("ensure swallowed a denial")
	}

	caps.mu.Lock()
	caps.getErr = nil
	caps.mu.Unlock()
	b2, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("ensure after outage cleared: %v", err)
	}
	if b2.CaptureID != b1.CaptureID {
		t.Fatalf("outage replaced the binding: %s -> %s", b1.CaptureID, b2.CaptureID)
	}
	if creates, _ := caps.counts(); creates != 1 {
		t.Fatalf("a transient fault minted %d captures", creates)
	}
}

// CAPINT-02: only a genuinely-gone capture (410/404/capture_gone) lets
// Ensure mint a replacement — under the SAME durable scope anchor.
func TestEnsureCaptureGoneRebindsSameAnchor(t *testing.T) {
	h := setup(t, returnsession.Config{})
	caps := newFakeCaps()
	sid, grant := sealedLocal(t, h, caps)

	b1, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	caps.kill(b1.CaptureID) // the filesvc released/expired it server-side

	b2, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("rebind after genuine loss: %v", err)
	}
	if b2.CaptureID == b1.CaptureID {
		t.Fatal("a gone capture was re-answered as live")
	}
	if b2.ScopeID != b1.ScopeID {
		t.Fatalf("rebind changed the scope anchor: %s -> %s", b1.ScopeID, b2.ScopeID)
	}
	anchor, cid, _ := bindingRow(t, h, sid)
	if anchor != b1.ScopeID || cid != b2.CaptureID {
		t.Fatalf("persisted row anchor=%s capture=%s, want anchor=%s capture=%s",
			anchor, cid, b1.ScopeID, b2.CaptureID)
	}
	caps.mu.Lock()
	last := caps.expected[len(caps.expected)-1]
	caps.mu.Unlock()
	if last != b1.ScopeID {
		t.Fatalf("rebind did not expect the durable anchor: %q", last)
	}
}

// CAPINT-02: concurrent ensures converge on ONE minted capture — the
// persona-serialized transaction is the fence, not luck.
func TestEnsureCaptureConcurrentBindsOnce(t *testing.T) {
	h := setup(t, returnsession.Config{})
	caps := newFakeCaps()
	sid, grant := sealedLocal(t, h, caps)

	const n = 8
	var wg sync.WaitGroup
	ids := make(chan string, n)
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := h.sessions.EnsureCapture(context.Background(), sid, grant)
			if err != nil {
				errs <- err
				return
			}
			ids <- b.CaptureID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent ensure: %v", err)
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("concurrent ensures disagreed: %s vs %s", first, id)
		}
	}
	if creates, _ := caps.counts(); creates != 1 {
		t.Fatalf("%d racers minted %d captures", n, creates)
	}
}

// CAPINT-01: a caller that planned capture C1 cannot read anything once
// the association moved — not a page, not a byte — even though the
// grant, session and scope are all still valid.
func TestCaptureReadsRequirePlannedCapture(t *testing.T) {
	h := setup(t, returnsession.Config{})
	caps := newFakeCaps()
	sid, grant := sealedLocal(t, h, caps)

	b1, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if _, _, _, err := h.sessions.AuthorizeCaptureRead(h.ctx, sid, grant, ""); !errors.Is(err, returnsession.ErrBadRequest) {
		t.Fatalf("missing expected: %v (want ErrBadRequest)", err)
	}
	if _, _, _, err := h.sessions.AuthorizeCaptureRead(h.ctx, sid, grant, "cap-never-bound"); !errors.Is(err, returnsession.ErrCaptureReplaced) {
		t.Fatalf("foreign expected: %v (want ErrCaptureReplaced)", err)
	}
	if _, _, _, err := h.sessions.AuthorizeCaptureEntries(h.ctx, sid, grant, "cap-never-bound"); !errors.Is(err, returnsession.ErrCaptureReplaced) {
		t.Fatalf("foreign entries expected: %v (want ErrCaptureReplaced)", err)
	}
	if _, _, _, err := h.sessions.AuthorizeCaptureRead(h.ctx, sid, grant, b1.CaptureID); err != nil {
		t.Fatalf("planned read refused: %v", err)
	}

	// A valid retake moves the association; the old plan is dead.
	b2, err := h.sessions.RetakeCapture(h.ctx, sid, grant, b1.ScopeID, b1.CaptureID)
	if err != nil {
		t.Fatalf("retake: %v", err)
	}
	if !b2.Retaken || b2.CaptureID == b1.CaptureID {
		t.Fatalf("retake did not mint: %+v", b2)
	}
	if _, _, _, err := h.sessions.AuthorizeCaptureRead(h.ctx, sid, grant, b1.CaptureID); !errors.Is(err, returnsession.ErrCaptureReplaced) {
		t.Fatalf("stale reader reached the new manifest: %v", err)
	}
	if _, _, _, err := h.sessions.AuthorizeCaptureRead(h.ctx, sid, grant, b2.CaptureID); err != nil {
		t.Fatalf("current binding refused: %v", err)
	}
}

// CAPINT-02: a retake whose response was lost is retried with the OLD
// planned capture id — the service answers the already-committed
// replacement instead of minting a third capture or releasing it.
func TestRetakeLostResponseRecoversWithoutChurn(t *testing.T) {
	h := setup(t, returnsession.Config{})
	caps := newFakeCaps()
	sid, grant := sealedLocal(t, h, caps)

	b1, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// The retake commits C2 and releases C1 — then its answer is lost.
	b2, err := h.sessions.RetakeCapture(h.ctx, sid, grant, b1.ScopeID, b1.CaptureID)
	if err != nil {
		t.Fatalf("retake: %v", err)
	}
	// The retry knows only the stale plan (scope, C1).
	b3, err := h.sessions.RetakeCapture(h.ctx, sid, grant, b1.ScopeID, b1.CaptureID)
	if err != nil {
		t.Fatalf("lost-response retry: %v", err)
	}
	if b3.CaptureID != b2.CaptureID || !b3.Retaken {
		t.Fatalf("retry did not recover the committed binding: %+v want capture=%s retaken", b3, b2.CaptureID)
	}
	creates, releases := caps.counts()
	if creates != 2 {
		t.Fatalf("lost-response retry minted again: %d creates", creates)
	}
	if releases != 1 {
		t.Fatalf("release churn: %d releases %v", releases, caps.released)
	}
}

// CAPINT-03: a release minted against an older association arrives
// after a retake moved the binding — it must refuse, leaving the newer
// capture AND the scope anchor intact.
func TestDelayedReleaseCannotClearReplacement(t *testing.T) {
	h := setup(t, returnsession.Config{})
	caps := newFakeCaps()
	sid, grant := sealedLocal(t, h, caps)

	b1, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	b2, err := h.sessions.RetakeCapture(h.ctx, sid, grant, b1.ScopeID, b1.CaptureID)
	if err != nil {
		t.Fatalf("retake: %v", err)
	}
	// The delayed C1 release lands now — against a moved binding.
	if err := h.sessions.ReleaseCapture(h.ctx, sid, grant, b1.CaptureID); !errors.Is(err, returnsession.ErrCaptureReplaced) {
		t.Fatalf("stale release: %v (want ErrCaptureReplaced)", err)
	}
	anchor, cid, _ := bindingRow(t, h, sid)
	if cid != b2.CaptureID || anchor != b1.ScopeID {
		t.Fatalf("delayed release damaged the binding: anchor=%s capture=%s", anchor, cid)
	}
	// The current binding's own release works — and keeps the anchor.
	if err := h.sessions.ReleaseCapture(h.ctx, sid, grant, b2.CaptureID); err != nil {
		t.Fatalf("release current: %v", err)
	}
	anchor, cid, _ = bindingRow(t, h, sid)
	if cid != "" || anchor != b1.ScopeID {
		t.Fatalf("release lost the anchor or kept the capture: anchor=%s capture=%s", anchor, cid)
	}
	// A re-bind after release still answers under the original anchor.
	b3, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("rebind: %v", err)
	}
	if b3.ScopeID != b1.ScopeID {
		t.Fatalf("post-release bind changed trees: %s -> %s", b1.ScopeID, b3.ScopeID)
	}
}

// CAPINT-03: an unnamed release (an old client or terminal cleanup
// path) is serialized the same way — it clears exactly what was bound
// at decision time, and the anchor still survives.
func TestUnnamedReleaseClearsBoundKeepsAnchor(t *testing.T) {
	h := setup(t, returnsession.Config{})
	caps := newFakeCaps()
	sid, grant := sealedLocal(t, h, caps)

	b1, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := h.sessions.ReleaseCapture(h.ctx, sid, grant, ""); err != nil {
		t.Fatalf("unnamed release: %v", err)
	}
	anchor, cid, _ := bindingRow(t, h, sid)
	if cid != "" || anchor != b1.ScopeID {
		t.Fatalf("unnamed release: anchor=%s capture=%s", anchor, cid)
	}
	// Idempotent — a second release on an unbound session answers.
	if err := h.sessions.ReleaseCapture(h.ctx, sid, grant, ""); err != nil {
		t.Fatalf("release again: %v", err)
	}
}

// CAPINT-03: cancelling the sealed session resolves its binding through
// the serialized cleanup — the capture reservation is released but the
// durable scope anchor stays on the row.
func TestCancelKeepsScopeAnchor(t *testing.T) {
	h := setup(t, returnsession.Config{})
	caps := newFakeCaps()
	sid, grant := sealedLocal(t, h, caps)

	b1, err := h.sessions.EnsureCapture(h.ctx, sid, grant)
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	code, raw := h.ownerReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sid), nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	anchor, cid, _ := bindingRow(t, h, sid)
	if cid != "" {
		t.Fatalf("cancel left a live association: %s", cid)
	}
	if anchor != b1.ScopeID {
		t.Fatalf("cancel discarded the scope anchor: %q want %s", anchor, b1.ScopeID)
	}
	caps.mu.Lock()
	released := len(caps.released) > 0 && caps.released[len(caps.released)-1] == b1.CaptureID
	caps.mu.Unlock()
	if !released {
		t.Fatalf("cancel never released the capture reservation: %v", caps.released)
	}
}
