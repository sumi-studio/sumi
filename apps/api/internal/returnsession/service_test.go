package returnsession_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentstate"
	"github.com/sumi-studio/sumi/apps/api/internal/db"
	"github.com/sumi-studio/sumi/apps/api/internal/portable"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession"
	"github.com/sumi-studio/sumi/apps/api/internal/returnsession/returnsessiontest"
	"github.com/sumi-studio/sumi/apps/api/internal/testdb"
)

// placement is one real PostgreSQL database. The Cloud source uses
// SUMI_TEST_DB_URL; the Local destination uses SUMI_RETURN_DEST_DB_URL when
// set, so the two can run on separate PostgreSQL servers.
type placement struct {
	pool  *pgxpool.Pool
	state *agentstate.Store
	svc   *portable.Service
}

func newPlacement(t *testing.T, dest bool) placement {
	t.Helper()
	if d := os.Getenv("SUMI_RETURN_DEST_DB_URL"); dest && d != "" && os.Getenv("SUMI_TEST_DB_URL") != "" {
		src := os.Getenv("SUMI_TEST_DB_URL")
		os.Setenv("SUMI_TEST_DB_URL", d)
		defer os.Setenv("SUMI_TEST_DB_URL", src)
	}
	pool := testdb.Create(t)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return placement{pool: pool, state: agentstate.NewStore(pool), svc: portable.NewService(pool)}
}

func newID(t *testing.T) string {
	t.Helper()
	return uuid.Must(uuid.NewV7()).String()
}

// harness: the Cloud placement is the return's SOURCE — it hosts the
// session service and its routes. The Local placement is the destination
// the mover acts on.
type harness struct {
	t        *testing.T
	ctx      context.Context
	cloud    placement
	local    placement
	sessions *returnsession.Service
	proof    *returnsessiontest.Proof
	srv      *httptest.Server
	human    string
	persona  string
	owner    returnsession.Owner
}

func setup(t *testing.T, cfg returnsession.Config) *harness { return setupOn(t, cfg, true) }

// setupOn builds the fixture; cloudHome=false leaves the secretary off
// Cloud entirely so a test can run the forward move itself (the
// same-install reclaim path).
func setupOn(t *testing.T, cfg returnsession.Config, cloudHome bool) *harness {
	t.Helper()
	ctx := context.Background()
	h := &harness{t: t, ctx: ctx, cloud: newPlacement(t, false), local: newPlacement(t, true)}
	h.human, h.persona = newID(t), newID(t)
	mustExec(t, h.cloud.pool, `INSERT INTO humans (human_id) VALUES ($1)`, h.human)
	if cloudHome {
		if _, _, err := h.cloud.state.EnsurePersona(ctx, h.persona, nil, "Cloud secretary"); err != nil {
			t.Fatal(err)
		}
		mustExec(t, h.cloud.pool, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`, h.human, h.persona)
		if _, _, err := h.cloud.state.SubmitInput(ctx, &agentstate.Input{
			PersonaID: h.persona, InputID: "in-carried", Kind: "message",
			Payload: map[string]any{"text": "明日 9 時に会議"}, ActorKind: "human", ActorID: "owner", SourceSurface: "test",
		}); err != nil {
			t.Fatal(err)
		}
	}
	h.owner = returnsession.Owner{HumanID: h.human, PersonaID: h.persona}
	// FIXTURE POLICY: the common-flow tests run under the explicit
	// test-only FilePolicyFixture — it stands in for no product answer.
	// The undecided-policy gate has its own tests (TestFilePolicyUndecided*).
	cfg.FilePolicy = returnsession.FilePolicyFixture
	h.sessions = returnsession.New(h.cloud.pool, cfg)
	h.proof = returnsessiontest.NewProof()
	h.proof.Add("owner-cookie", h.human, h.persona)
	h.muxSrv(t)
	return h
}

func (h *harness) muxSrv(t *testing.T) {
	mux := http.NewServeMux()
	h.srv = httptest.NewServer(mux)
	t.Cleanup(h.srv.Close)
	server, err := returnsession.NewServer(h.sessions, h.proof, h.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterRoutes(mux)
}

func mustExec(t *testing.T, pool *pgxpool.Pool, q string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), q, args...); err != nil {
		t.Fatalf("exec: %v", err)
	}
}

func authority(t *testing.T, p placement, pid string) string {
	t.Helper()
	var a string
	err := p.pool.QueryRow(context.Background(), `SELECT authority FROM core_personas WHERE persona_id = $1`, pid).Scan(&a)
	if errors.Is(err, pgx.ErrNoRows) {
		return "absent"
	}
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (h *harness) request(method, path string, headers map[string]string, body io.Reader) (int, []byte) {
	h.t.Helper()
	req, err := http.NewRequestWithContext(h.ctx, method, h.srv.URL+path, body)
	if err != nil {
		h.t.Fatal(err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, raw
}

func (h *harness) ownerReq(method, path string, body io.Reader) (int, []byte) {
	return h.request(method, path, map[string]string{"Cookie": returnsessiontest.Cookie + "=owner-cookie"}, body)
}

func (h *harness) grantReq(method, path, grant string, body io.Reader) (int, []byte) {
	return h.request(method, path, map[string]string{"Authorization": "Bearer " + grant}, body)
}

func jsonBody(v any) io.Reader {
	raw, _ := json.Marshal(v)
	return bytes.NewReader(raw)
}

func asView(t *testing.T, raw []byte) returnsession.View {
	t.Helper()
	var env struct {
		Session *returnsession.View `json:"session"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	if env.Session != nil {
		return *env.Session
	}
	var v returnsession.View
	_ = json.Unmarshal(raw, &v)
	return v
}

// create opens a session over HTTP and returns the return URL and grant.
func (h *harness) create() (sessionID, returnURL, grant string) {
	h.t.Helper()
	code, raw := h.ownerReq(http.MethodPost, "/api/secretary-return/sessions", jsonBody(map[string]string{"file_mode": "cloud"}))
	if code != http.StatusCreated {
		h.t.Fatalf("create: %d %s", code, raw)
	}
	var body struct {
		ReturnURL string             `json:"return_url"`
		Session   returnsession.View `json:"session"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.ReturnURL == "" {
		h.t.Fatalf("create answer: %s", raw)
	}
	parts := strings.SplitN(body.ReturnURL, "#grant=", 2)
	if len(parts) != 2 || parts[1] == "" {
		h.t.Fatalf("return URL has no grant: %s", body.ReturnURL)
	}
	if !strings.HasSuffix(parts[0], "/api/secretary-return/sessions/"+body.Session.SessionID) {
		h.t.Fatalf("return URL %q does not name the session", parts[0])
	}
	return body.Session.SessionID, parts[0], parts[1]
}

func dest(t *testing.T, p placement, personaID, slot string) returnsession.Destination {
	t.Helper()
	own, err := p.svc.PlacementID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return returnsession.Destination{PlacementID: own, PersonaID: personaID, SlotState: slot, FileMode: "cloud"}
}

// drive is the destination-side happy path the mover performs: bind+seal,
// download, import, activate, report.
func (h *harness) driveImport(t *testing.T, sessionID, grant, slotState, slotPersona string, supersedes string) (returnsession.View, portable.Receipt) {
	t.Helper()
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, slotPersona, slotState)))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	v := asView(t, raw)
	if v.Status != returnsession.StatusSealed {
		t.Fatalf("after bind the session is %s", v.Status)
	}
	code, bundle := h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s/bundle", sessionID), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("download: %d %s", code, bundle)
	}
	var rec portable.Receipt
	var err error
	if supersedes != "" {
		rec, _, err = h.local.svc.ImportReturning(h.ctx, bytes.NewReader(bundle), nil, false, supersedes)
	} else {
		rec, _, err = h.local.svc.Import(h.ctx, bytes.NewReader(bundle), nil, false)
	}
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	act, err := h.local.svc.Activate(h.ctx, rec.PersonaID, sessionID)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/activated", sessionID),
		grant, jsonBody(map[string]string{"activate_proof": act.ActivateProof}))
	if code != http.StatusOK {
		t.Fatalf("report activated: %d %s", code, raw)
	}
	return asView(t, raw), rec
}

func TestReturnHappyPathFreshTarget(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()

	v, rec := h.driveImport(t, sessionID, grant, "absent", h.persona, "")
	if v.Status != returnsession.StatusCompleted {
		t.Fatalf("session %s", v.Status)
	}
	if got := authority(t, h.cloud, h.persona); got != "transferred" {
		t.Fatalf("cloud authority %s", got)
	}
	if got := authority(t, h.local, rec.PersonaID); got != "active" {
		t.Fatalf("local authority %s", got)
	}
	// The carried input travelled.
	var st string
	if err := h.local.pool.QueryRow(h.ctx, `SELECT status FROM core_inputs WHERE persona_id = $1 AND input_id = 'in-carried'`, rec.PersonaID).Scan(&st); err != nil || st != "queued" {
		t.Fatalf("carried input: %v %s", err, st)
	}
	// The owner's view reports the outcome without the transfer key.
	code, raw := h.ownerReq(http.MethodGet, "/api/secretary-return/session", nil)
	if code != http.StatusOK {
		t.Fatalf("owner status: %d %s", code, raw)
	}
	ov := asView(t, raw)
	if ov.Status != returnsession.StatusCompleted || ov.TransferKey != "" {
		t.Fatalf("owner view: %+v", ov)
	}
}

func TestReturnToSurrenderedInstallReclaimsSamePersona(t *testing.T) {
	h := setupOn(t, returnsession.Config{}, false)
	ctx := h.ctx

	// The full forward move first: this Local install owned the secretary
	// and surrendered it to Cloud — its slot now holds a transferred copy.
	local, cloud2 := h.local, h.cloud
	forwardID := newID(t)
	destID, err := cloud2.svc.PlacementID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := local.state.EnsurePersona(ctx, h.persona, nil, "Local secretary"); err != nil {
		t.Fatal(err)
	}
	if _, err := local.svc.Seal(ctx, h.persona, forwardID, destID); err != nil {
		t.Fatal(err)
	}
	var bundle bytes.Buffer
	if _, err := local.svc.Export(ctx, h.persona, forwardID, &bundle); err != nil {
		t.Fatal(err)
	}
	rec, _, err := cloud2.svc.Import(ctx, &bundle, &h.human, true)
	if err != nil {
		t.Fatal(err)
	}
	act, err := cloud2.svc.Activate(ctx, rec.PersonaID, forwardID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := local.svc.Complete(ctx, h.persona, forwardID, act.ActivateProof); err != nil {
		t.Fatal(err)
	}
	// Cloud persona is now the live continuation bound to the human.
	mustExec(t, h.cloud.pool, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`, h.human, h.persona)
	if got := authority(t, local, h.persona); got != "transferred" {
		t.Fatalf("local surrendered: %s", got)
	}

	h.local = local
	sessionID, _, grant := h.create()

	// The surrendered copy's hold is the forward transfer; the view names
	// it so the receiver's reclaim lineage is checkable.
	code, raw := h.grantReq(http.MethodGet, fmt.Sprintf("/api/secretary-return/sessions/%s", sessionID), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("status: %d %s", code, raw)
	}
	if v := asView(t, raw); v.SurrenderedBy != forwardID {
		t.Fatalf("surrendered_by %q, want %q", v.SurrenderedBy, forwardID)
	}

	v, rec := h.driveImport(t, sessionID, grant, "surrendered", h.persona, forwardID)
	if v.Status != returnsession.StatusCompleted {
		t.Fatalf("session %s", v.Status)
	}
	if rec.PersonaID != h.persona {
		t.Fatalf("reclaim minted %s, want same persona %s", rec.PersonaID, h.persona)
	}
	if got := authority(t, h.local, h.persona); got != "active" {
		t.Fatalf("home authority %s", got)
	}
	if got := authority(t, h.cloud, h.persona); got != "transferred" {
		t.Fatalf("cloud authority %s", got)
	}
}

func TestReturnWrongGrantAndWrongOwnerRefuse(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, sessionURL, _ := h.create()

	code, _ := h.grantReq(http.MethodGet, sessionURL[len(h.srv.URL):], "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", nil)
	if code != http.StatusUnauthorized {
		t.Fatalf("wrong grant: %d", code)
	}
	// A different signed-in owner sees no session.
	h.proof.Add("other-cookie", newID(t), newID(t))
	code, raw := h.request(http.MethodGet, "/api/secretary-return/session",
		map[string]string{"Cookie": returnsessiontest.Cookie + "=other-cookie"}, nil)
	if code != http.StatusNotFound {
		t.Fatalf("other owner: %d %s", code, raw)
	}
	code, raw = h.request(http.MethodPost, fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID),
		map[string]string{"Cookie": returnsessiontest.Cookie + "=other-cookie"}, nil)
	if code != http.StatusNotFound {
		t.Fatalf("other owner cancel: %d %s", code, raw)
	}
	// The owner reads their own open session back.
	if v, err := h.sessions.ForOwner(h.ctx, h.owner); err != nil || v.SessionID != sessionID {
		t.Fatalf("owner session: %v %+v", err, v)
	}
}

func TestReturnSecondPlacementRefusedBeforeSeal(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	d := dest(t, h.local, h.persona, "absent")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID), grant, jsonBody(d))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	// A second Local placement answering the same URL refuses; the source
	// is still sealed only for the first.
	other := newPlacement(t, true)
	d2 := dest(t, other, h.persona, "absent")
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID), grant, jsonBody(d2))
	if code != http.StatusConflict {
		t.Fatalf("second placement: %d %s", code, raw)
	}
	// The same placement re-binding is a resume, not an error.
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID), grant, jsonBody(d))
	if code != http.StatusOK {
		t.Fatalf("re-bind: %d %s", code, raw)
	}
}

func TestReturnSurrenderedSlotMustBeThisSecretary(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	// A slot declaring "surrendered" for a different persona refuses before
	// the seal — the return would not reach the secretary it is for.
	d := dest(t, h.local, newID(t), "surrendered")
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID), grant, jsonBody(d))
	if code != http.StatusConflict {
		t.Fatalf("mismatched surrender: %d %s", code, raw)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("the refusal sealed the source: %s", got)
	}
}

func TestReturnCancelBeforeSeal(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	code, raw := h.ownerReq(http.MethodPost, fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), nil)
	if code != http.StatusOK {
		t.Fatalf("owner cancel: %d %s", code, raw)
	}
	if v := asView(t, raw); v.Status != returnsession.StatusCancelled {
		t.Fatalf("status %s", v.Status)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("authority %s", got)
	}
	// The destination can no longer bind.
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, h.persona, "absent")))
	if code != http.StatusGone {
		t.Fatalf("bind after cancel: %d %s", code, raw)
	}
}

func TestReturnCancelAfterSealNeedsRetireProof(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, h.persona, "absent")))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	// The owner cancels after the seal: intent only, the source stays
	// sealed — it is NOT unsealed by the request.
	code, raw = h.ownerReq(http.MethodPost, fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	if v := asView(t, raw); v.Status != returnsession.StatusCancelling {
		t.Fatalf("status %s", v.Status)
	}
	if got := authority(t, h.cloud, h.persona); got != "sealed" {
		t.Fatalf("the cancel unsealed the source: %s", got)
	}
	// The grant holder's status still shows where the proof must go.
	code, raw = h.grantReq(http.MethodGet, fmt.Sprintf("/api/secretary-return/sessions/%s", sessionID), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("status: %d %s", code, raw)
	}
	v := asView(t, raw)
	if v.TransferKey == "" {
		t.Fatal("the cancelling view carries no transfer key for the tombstone")
	}
	// Tombstone the never-arrived transfer and report its proof.
	rec, err := h.local.svc.Retire(h.ctx, h.persona, sessionID, mustPlacement(t, h.local), v.TransferKey)
	if err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/retired", sessionID),
		grant, jsonBody(map[string]string{"retire_proof": rec.RetireProof}))
	if code != http.StatusOK {
		t.Fatalf("report retired: %d %s", code, raw)
	}
	if v := asView(t, raw); v.Status != returnsession.StatusAborted {
		t.Fatalf("status %s", v.Status)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("the source did not unseal on the proof: %s", got)
	}
}

func TestReturnCancelLosesToCommittedActivation(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, h.persona, "absent")))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	code, bundle := h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s/bundle", sessionID), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("download: %d", code)
	}
	rec, _, err := h.local.svc.Import(h.ctx, bytes.NewReader(bundle), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	act, err := h.local.svc.Activate(h.ctx, rec.PersonaID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	// The cancel request lands before the activation report: it can only
	// mark intent — the committed activation still completes the transfer.
	code, raw = h.ownerReq(http.MethodPost, fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), nil)
	if code != http.StatusOK || asView(t, raw).Status != returnsession.StatusCancelling {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	code, raw = h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/activated", sessionID),
		grant, jsonBody(map[string]string{"activate_proof": act.ActivateProof}))
	if code != http.StatusOK {
		t.Fatalf("activated report during cancelling: %d %s", code, raw)
	}
	if v := asView(t, raw); v.Status != returnsession.StatusCompleted {
		t.Fatalf("status %s", v.Status)
	}
	if got := authority(t, h.cloud, h.persona); got != "transferred" {
		t.Fatalf("cloud authority %s", got)
	}
}

func TestReturnLostReportsReplay(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	v, _ := h.driveImport(t, sessionID, grant, "absent", h.persona, "")
	if v.Status != returnsession.StatusCompleted {
		t.Fatalf("session %s", v.Status)
	}
	// A repeated activated report is a replay, not a conflict.
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/activated", sessionID),
		grant, jsonBody(map[string]string{"activate_proof": "wrong-proof"}))
	if code != http.StatusOK {
		t.Fatalf("replayed report: %d %s", code, raw)
	}
	// The session row falling behind the ledger (a lost update) is closed
	// by reconcile: completed in the ledger means completed in the view.
	mustExec(t, h.cloud.pool, `UPDATE return_sessions SET status = 'sealed' WHERE session_id = $1`, sessionID)
	code, raw = h.grantReq(http.MethodGet, fmt.Sprintf("/api/secretary-return/sessions/%s", sessionID), grant, nil)
	if code != http.StatusOK || asView(t, raw).Status != returnsession.StatusCompleted {
		t.Fatalf("reconciled status: %d %s", code, raw)
	}
}

func TestReturnSealCommittedWithoutSessionUpdate(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, h.persona, "absent")))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	// The bind committed and the seal committed but the status update was
	// lost: the row says awaiting_destination while the ledger is sealed.
	mustExec(t, h.cloud.pool, `UPDATE return_sessions SET status = 'awaiting_destination' WHERE session_id = $1`, sessionID)
	code, raw = h.grantReq(http.MethodGet, fmt.Sprintf("/api/secretary-return/sessions/%s", sessionID), grant, nil)
	if code != http.StatusOK || asView(t, raw).Status != returnsession.StatusSealed {
		t.Fatalf("reconciled: %d %s", code, raw)
	}
}

func TestReturnAdmissionExpiryIsNotAuthorityEvidence(t *testing.T) {
	h := setup(t, returnsession.Config{AdmitTTL: 50 * time.Millisecond})
	sessionID, _, grant := h.create()
	time.Sleep(80 * time.Millisecond)
	// The deadline stops new admission: bind refuses.
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, h.persona, "absent")))
	if code != http.StatusGone {
		t.Fatalf("bind after deadline: %d %s", code, raw)
	}
	code, raw = h.grantReq(http.MethodGet, fmt.Sprintf("/api/secretary-return/sessions/%s", sessionID), grant, nil)
	if code != http.StatusOK || asView(t, raw).Status != returnsession.StatusExpired {
		t.Fatalf("expired view: %d %s", code, raw)
	}
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("expiry changed authority: %s", got)
	}
}

func TestReturnSealedSessionOutlivesDeadline(t *testing.T) {
	h := setup(t, returnsession.Config{AdmitTTL: 300 * time.Millisecond})
	sessionID, _, grant := h.create()
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, h.persona, "absent")))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	time.Sleep(400 * time.Millisecond)
	// Past the deadline, a sealed session still completes on proof.
	v, _ := h.driveImportRest(t, sessionID, grant)
	if v.Status != returnsession.StatusCompleted {
		t.Fatalf("status %s", v.Status)
	}
}

// driveImportRest continues a session that is already bound+sealed:
// download, import, activate, report.
func (h *harness) driveImportRest(t *testing.T, sessionID, grant string) (returnsession.View, portable.Receipt) {
	t.Helper()
	code, bundle := h.grantReq(http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s/bundle", sessionID), grant, nil)
	if code != http.StatusOK {
		t.Fatalf("download: %d", code)
	}
	rec, _, err := h.local.svc.Import(h.ctx, bytes.NewReader(bundle), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	act, err := h.local.svc.Activate(h.ctx, rec.PersonaID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/activated", sessionID),
		grant, jsonBody(map[string]string{"activate_proof": act.ActivateProof}))
	if code != http.StatusOK {
		t.Fatalf("report: %d %s", code, raw)
	}
	return asView(t, raw), rec
}

func TestReturnCreateRefusesUnownedAndDuplicate(t *testing.T) {
	h := setup(t, returnsession.Config{})
	// A claim pointing at a persona the account does not own refuses.
	other := returnsession.Owner{HumanID: h.human, PersonaID: newID(t)}
	if _, _, err := h.sessions.Create(h.ctx, other, "cloud"); !errors.Is(err, returnsession.ErrNoSecretary) {
		t.Fatalf("unowned persona: %v", err)
	}
	stranger, strangerPersona := newID(t), newID(t)
	mustExec(t, h.cloud.pool, `INSERT INTO humans (human_id) VALUES ($1)`, stranger)
	if _, _, err := h.cloud.state.EnsurePersona(h.ctx, strangerPersona, nil, "other"); err != nil {
		t.Fatal(err)
	}
	mustExec(t, h.cloud.pool, `UPDATE core_personas SET human_id = $1 WHERE persona_id = $2`, stranger, strangerPersona)
	if _, _, err := h.sessions.Create(h.ctx, returnsession.Owner{HumanID: h.human, PersonaID: strangerPersona}, "cloud"); !errors.Is(err, returnsession.ErrNotOwned) {
		t.Fatalf("someone else's persona: %v", err)
	}
	// One open session per secretary: the second create names the first.
	h.create()
	created2, open, err := h.sessions.Create(h.ctx, h.owner, "cloud")
	if !errors.Is(err, returnsession.ErrOpenSession) || open == "" {
		t.Fatalf("duplicate: %v %q %+v", err, open, created2)
	}
}

func mustPlacement(t *testing.T, p placement) string {
	t.Helper()
	id, err := p.svc.PlacementID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// gatedServer mounts the return routes over a service whose file policy is
// undecided — the production shape while the user's decision is open: the
// routes exist (the shared public base URL is set) but no new move is
// admitted.
func gatedServer(t *testing.T, h *harness) *httptest.Server {
	t.Helper()
	svc := returnsession.New(h.cloud.pool, returnsession.Config{})
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	gs, err := returnsession.NewServer(svc, h.proof, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	gs.RegisterRoutes(mux)
	return srv
}

func (h *harness) req(t *testing.T, srv *httptest.Server, method, path, cookie, grant string, body io.Reader) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(h.ctx, method, srv.URL+path, body)
	if err != nil {
		t.Fatal(err)
	}
	if cookie != "" {
		req.Header.Set("Cookie", cookie)
	}
	if grant != "" {
		req.Header.Set("Authorization", "Bearer "+grant)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	return res.StatusCode, raw
}

// TestFilePolicyUndecidedRefusesNewMove is the production boundary: with
// the file decision unanswered, creating a session is refused, binding a
// pre-existing session's destination is refused, and nothing about
// authority or destination state changes — while status stays readable
// and cancel still works.
func TestFilePolicyUndecidedRefusesNewMove(t *testing.T) {
	h := setup(t, returnsession.Config{})
	srv := gatedServer(t, h)
	cookie := returnsessiontest.Cookie + "=owner-cookie"

	// Owner create over real HTTP → 409 carrying the machine marker.
	code, raw := h.req(t, srv, http.MethodPost, "/api/secretary-return/sessions", cookie, "", nil)
	if code != http.StatusConflict {
		t.Fatalf("create: %d %s", code, raw)
	}
	var e struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	if err := json.Unmarshal(raw, &e); err != nil || e.Code != "file_policy_undecided" {
		t.Fatalf("create refusal: %s", raw)
	}
	// The owner session read is honest: no session exists.
	code, _ = h.req(t, srv, http.MethodGet, "/api/secretary-return/session", cookie, "", nil)
	if code != http.StatusNotFound {
		t.Fatalf("owner read: %d", code)
	}

	// A session minted while the policy was enabled (the fixture service —
	// the only way a session can exist) is still refused at the seal: the
	// destination bind is the admission boundary, not the create alone.
	created, _, err := h.sessions.Create(h.ctx, h.owner, "cloud")
	if err != nil {
		t.Fatal(err)
	}
	sessionID := created.View.SessionID
	d := dest(t, h.local, newID(t), "absent")
	code, raw = h.req(t, srv, http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		"", created.Grant, jsonBody(d))
	if code != http.StatusConflict {
		t.Fatalf("bind: %d %s", code, raw)
	}
	// Nothing moved: source authority, destination columns, import ledger.
	if got := authority(t, h.cloud, h.persona); got != "active" {
		t.Fatalf("cloud authority %s", got)
	}
	var bound *string
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT destination_placement_id::text FROM return_sessions WHERE session_id = $1`,
		sessionID).Scan(&bound); err != nil || bound != nil {
		t.Fatalf("destination bound anyway: %v %v", err, bound)
	}
	if _, err := h.cloud.svc.Status(h.ctx, "export", sessionID); !errors.Is(err, portable.ErrTransferNotFound) {
		t.Fatalf("export staged anyway: %v", err)
	}
	// Status is readable to the grant holder, and the owner's cancel
	// closes the open session honestly.
	code, _ = h.req(t, srv, http.MethodGet,
		fmt.Sprintf("/api/secretary-return/sessions/%s", sessionID), "", created.Grant, nil)
	if code != http.StatusOK {
		t.Fatalf("grant status: %d", code)
	}
	code, _ = h.req(t, srv, http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/cancel", sessionID), cookie, "", nil)
	if code != http.StatusOK {
		t.Fatalf("owner cancel: %d", code)
	}
	var status string
	if err := h.cloud.pool.QueryRow(h.ctx,
		`SELECT status FROM return_sessions WHERE session_id = $1`, sessionID).Scan(&status); err != nil || status != "cancelled" {
		t.Fatalf("cancel: %v %s", err, status)
	}
}

// TestFilePolicyUndecidedDoesNotStrandBegunWork: a session already sealed
// while the policy was enabled keeps resolving under an undecided-policy
// service — download, activation report and cancel are recovery, not new
// admission, so the gate never strands moved authority.
func TestFilePolicyUndecidedDoesNotStrandBegunWork(t *testing.T) {
	h := setup(t, returnsession.Config{})
	sessionID, _, grant := h.create()
	code, raw := h.grantReq(http.MethodPost,
		fmt.Sprintf("/api/secretary-return/sessions/%s/destination", sessionID),
		grant, jsonBody(dest(t, h.local, newID(t), "absent")))
	if code != http.StatusOK {
		t.Fatalf("bind: %d %s", code, raw)
	}
	if got := authority(t, h.cloud, h.persona); got != "sealed" {
		t.Fatalf("source %s", got)
	}

	// The same rows, an undecided service: the gated code path now.
	gated := returnsession.New(h.cloud.pool, returnsession.Config{})
	var bundle bytes.Buffer
	if _, err := gated.Download(h.ctx, sessionID, grant, &bundle); err != nil {
		t.Fatalf("download under undecided policy: %v", err)
	}
	rec, _, err := h.local.svc.Import(h.ctx, &bundle, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	act, err := h.local.svc.Activate(h.ctx, rec.PersonaID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := gated.ReportActivated(h.ctx, sessionID, grant, act.ActivateProof)
	if err != nil || v.Status != returnsession.StatusCompleted {
		t.Fatalf("report under undecided policy: %v %+v", err, v)
	}
	if got := authority(t, h.cloud, h.persona); got != "transferred" {
		t.Fatalf("cloud authority %s", got)
	}
	if got := authority(t, h.local, rec.PersonaID); got != "active" {
		t.Fatalf("local authority %s", got)
	}
}
